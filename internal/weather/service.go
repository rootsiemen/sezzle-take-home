package weather

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"

	"weatherlookup/internal/admission"
	"weatherlookup/internal/cache"
	"weatherlookup/internal/validation"
)

type Provider interface {
	Search(context.Context, string) (Location, error)
	CurrentWeather(context.Context, float64, float64) (Location, CurrentWeather, error)
}

type Service struct {
	provider         Provider
	cache            *cache.Cache[Result]
	maxAttempts      int
	maxStaleAge      time.Duration
	fillTimeout      time.Duration
	retryDelay       func(int) time.Duration
	now              func() time.Time
	observer         Observer
	flights          singleflight.Group
	vendorSlots      *admission.Limiter
	guards           GuardObserver
	geocodingBreaker *circuitBreaker
	forecastBreaker  *circuitBreaker
}

func NewService(provider Provider) *Service {
	return NewServiceWithOptions(provider, Options{})
}

// Options controls resilience and instrumentation without changing the
// original NewService constructor used by library callers and tests.
type Options struct {
	Cache *cache.Cache[Result]
	// MaxStaleAge is total age since cache insertion, including the fresh TTL.
	// Zero selects 15 minutes. A negative value disables stale fallback.
	MaxStaleAge time.Duration
	// FillTimeout bounds a shared cache fill independently of any one caller.
	// Abandoned fills may warm the cache until this timeout (default 5s).
	FillTimeout time.Duration
	// MaxVendorInFlight bounds logical vendor-backed lookups, including all
	// retries/backoff and detached cache fills (default 8), without queueing.
	MaxVendorInFlight       int
	BreakerFailureThreshold int
	BreakerCooldown         time.Duration
	MaxAttempts             int
	RetryDelay              func(attempt int) time.Duration
	Observer                Observer
	Now                     func() time.Time
}

// Observer receives bounded-cardinality service events for Prometheus and
// other monitoring implementations.
type Observer interface {
	CacheLookup(result string)
	CacheStats(entries int, evictions uint64)
	VendorRequest(operation, outcome string, duration time.Duration)
	VendorRetry(operation string)
	StaleResponse()
}

func NewServiceWithOptions(provider Provider, options Options) *Service {
	maxAttempts := options.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 2
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.RetryDelay == nil {
		options.RetryDelay = defaultRetryDelay
	}
	if options.MaxStaleAge == 0 {
		options.MaxStaleAge = 15 * time.Minute
	}
	if options.FillTimeout <= 0 {
		options.FillTimeout = 5 * time.Second
	}
	if options.MaxVendorInFlight <= 0 {
		options.MaxVendorInFlight = 8
	}
	if options.BreakerFailureThreshold <= 0 {
		options.BreakerFailureThreshold = 5
	}
	if options.BreakerCooldown <= 0 {
		options.BreakerCooldown = 30 * time.Second
	}
	guards, _ := options.Observer.(GuardObserver)
	var observeSlots func(int, int)
	if guards != nil {
		observeSlots = guards.VendorAdmission
	}
	return &Service{
		provider:         provider,
		cache:            options.Cache,
		maxAttempts:      maxAttempts,
		maxStaleAge:      options.MaxStaleAge,
		fillTimeout:      options.FillTimeout,
		retryDelay:       options.RetryDelay,
		now:              options.Now,
		observer:         options.Observer,
		guards:           guards,
		vendorSlots:      admission.New(options.MaxVendorInFlight, observeSlots),
		geocodingBreaker: newCircuitBreaker("geocoding.search", options.BreakerFailureThreshold, options.BreakerCooldown, options.Now, guards),
		forecastBreaker:  newCircuitBreaker("forecast.current", options.BreakerFailureThreshold, options.BreakerCooldown, options.Now, guards),
	}
}

type CacheStatus string

const (
	CacheDisabled CacheStatus = "disabled"
	CacheHit      CacheStatus = "hit"
	CacheMiss     CacheStatus = "miss"
	CacheStale    CacheStatus = "stale"
)

type LookupMetadata struct {
	CacheStatus CacheStatus
	Age         time.Duration
}

type Query struct {
	Latitude  *float64
	Longitude *float64
	Location  string
}

type Location struct {
	Name        string  `json:"name,omitempty"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	Timezone    string  `json:"timezone,omitempty"`
	Country     string  `json:"country,omitempty"`
	CountryCode string  `json:"country_code,omitempty"`
	Admin1      string  `json:"admin1,omitempty"`
}

type CurrentWeather struct {
	Time                 string  `json:"time"`
	TemperatureC         float64 `json:"temperature_c"`
	RelativeHumidityPct  float64 `json:"relative_humidity_pct"`
	ApparentTemperatureC float64 `json:"apparent_temperature_c"`
	IsDay                bool    `json:"is_day"`
	PrecipitationMM      float64 `json:"precipitation_mm"`
	RainMM               float64 `json:"rain_mm"`
	ShowersMM            float64 `json:"showers_mm"`
	SnowfallCM           float64 `json:"snowfall_cm"`
	WeatherCode          int     `json:"weather_code"`
	Condition            string  `json:"condition"`
	CloudCoverPct        float64 `json:"cloud_cover_pct"`
	WindSpeedKmh         float64 `json:"wind_speed_kmh"`
	WindDirectionDeg     float64 `json:"wind_direction_deg"`
}

// Current is the normalized value returned by a Provider. Provider adapters
// map their own response format into this service-owned type.
type Current = CurrentWeather

type Result struct {
	Location Location       `json:"location"`
	Current  CurrentWeather `json:"current"`
}

func (s *Service) Lookup(ctx context.Context, query Query) (Result, error) {
	result, _, err := s.LookupWithMetadata(ctx, query)
	return result, err
}

func (s *Service) LookupWithMetadata(ctx context.Context, query Query) (Result, LookupMetadata, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, LookupMetadata{}, err
	}
	if s == nil || s.provider == nil {
		return Result{}, LookupMetadata{}, errors.New("weather provider is not configured")
	}
	location, err := validation.Location(query.Location)
	if err != nil {
		return Result{}, LookupMetadata{CacheStatus: CacheDisabled}, err
	}
	query.Location = location
	if query.Location != "" && (query.Latitude != nil || query.Longitude != nil) {
		return Result{}, LookupMetadata{CacheStatus: CacheDisabled}, errors.New("use either location or latitude/longitude, not both")
	}
	if query.Location == "" {
		if query.Latitude == nil || query.Longitude == nil {
			return Result{}, LookupMetadata{CacheStatus: CacheDisabled}, errors.New("latitude and longitude are required")
		}
		if *query.Latitude < -90 || *query.Latitude > 90 {
			return Result{}, LookupMetadata{CacheStatus: CacheDisabled}, errors.New("latitude must be between -90 and 90")
		}
		if *query.Longitude < -180 || *query.Longitude > 180 {
			return Result{}, LookupMetadata{CacheStatus: CacheDisabled}, errors.New("longitude must be between -180 and 180")
		}
	}

	metadata := LookupMetadata{CacheStatus: CacheDisabled}
	key := cacheKey(query)
	if s.cache == nil || key == "" {
		result, err := s.lookupFresh(ctx, query)
		return result, metadata, err
	}

	if result, age, ok := s.cache.Get(key); ok {
		s.observeCache(string(CacheHit))
		s.observeCacheStats()
		return result, LookupMetadata{CacheStatus: CacheHit, Age: age}, nil
	}
	s.observeCache(string(CacheMiss))
	s.observeCacheStats()
	metadata.CacheStatus = CacheMiss

	completed := s.flights.DoChan(key, func() (any, error) {
		// Recheck after joining the flight: another request may have filled the
		// cache between the initial miss and this function acquiring the key.
		if result, age, ok := s.cache.Get(key); ok {
			return lookupValue{result: result, metadata: LookupMetadata{CacheStatus: CacheHit, Age: age}}, nil
		}
		// Retain trace values, not the first caller's cancellation/deadline.
		fillContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.fillTimeout)
		defer cancel()
		result, err := s.lookupFresh(fillContext, query)
		if err != nil {
			return nil, err
		}
		stats := s.cache.Set(key, result)
		if s.observer != nil {
			s.observer.CacheStats(stats.Entries, stats.Evictions)
		}
		return lookupValue{result: result, metadata: metadata}, nil
	})
	select {
	case <-ctx.Done():
		return Result{}, metadata, ctx.Err()
	case value := <-completed:
		// Prefer cancellation if it raced with completion.
		if err := ctx.Err(); err != nil {
			return Result{}, metadata, err
		}
		if value.Err != nil {
			// Evaluate freshness after retries, separately for each live caller.
			return s.staleOrError(key, metadata, value.Err)
		}
		result := value.Val.(lookupValue)
		return result.result, result.metadata, nil
	}
}

type lookupValue struct {
	result   Result
	metadata LookupMetadata
}

func (s *Service) lookupFresh(ctx context.Context, query Query) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if !s.vendorSlots.TryAcquire() {
		if s.guards != nil {
			s.guards.VendorRejected()
		}
		return Result{}, ErrVendorBusy
	}
	// The slot belongs to the fill, not a waiting HTTP caller. Disconnection
	// cannot release it while detached vendor work is still in flight.
	defer s.vendorSlots.Release()
	var location Location
	if query.Location != "" {
		var err error
		location, err = s.search(ctx, query.Location)
		if err != nil {
			return Result{}, err
		}
	} else {
		location = Location{Latitude: *query.Latitude, Longitude: *query.Longitude}
	}

	weatherLocation, current, err := s.current(ctx, location.Latitude, location.Longitude)
	if err != nil {
		return Result{}, err
	}

	resultLocation := Location{
		Name:        location.Name,
		Latitude:    weatherLocation.Latitude,
		Longitude:   weatherLocation.Longitude,
		Timezone:    weatherLocation.Timezone,
		Country:     location.Country,
		CountryCode: location.CountryCode,
		Admin1:      location.Admin1,
	}
	if resultLocation.Latitude == 0 && resultLocation.Longitude == 0 && (location.Latitude != 0 || location.Longitude != 0) {
		resultLocation.Latitude = location.Latitude
		resultLocation.Longitude = location.Longitude
	}
	if resultLocation.Timezone == "" {
		resultLocation.Timezone = location.Timezone
	}
	current.Condition = conditionForCode(current.WeatherCode)

	return Result{Location: resultLocation, Current: current}, nil
}

func (s *Service) search(ctx context.Context, name string) (Location, error) {
	return retry(ctx, "geocoding.search", s.maxAttempts, s.retryDelay, s.now, s.observer,
		func(ctx context.Context) (Location, error) {
			return throughCircuit(ctx, s.geocodingBreaker, func(ctx context.Context) (Location, error) {
				return s.provider.Search(ctx, name)
			})
		})
}

type currentResult struct {
	location Location
	current  CurrentWeather
}

func (s *Service) current(ctx context.Context, latitude, longitude float64) (Location, CurrentWeather, error) {
	value, err := retry(ctx, "forecast.current", s.maxAttempts, s.retryDelay, s.now, s.observer,
		func(ctx context.Context) (currentResult, error) {
			return throughCircuit(ctx, s.forecastBreaker, func(ctx context.Context) (currentResult, error) {
				location, current, err := s.provider.CurrentWeather(ctx, latitude, longitude)
				return currentResult{location: location, current: current}, err
			})
		})
	return value.location, value.current, err
}

func (s *Service) staleOrError(key string, metadata LookupMetadata, err error) (Result, LookupMetadata, error) {
	staleResult, staleAge, hasStale := s.cache.GetStale(key)
	if hasStale && staleAge <= s.maxStaleAge && (retryable(err) || errors.Is(err, ErrCircuitOpen) || errors.Is(err, ErrVendorBusy)) {
		if s.observer != nil {
			s.observer.StaleResponse()
		}
		metadata.CacheStatus = CacheStale
		metadata.Age = staleAge
		return staleResult, metadata, nil
	}
	return Result{}, metadata, err
}

func cacheKey(query Query) string {
	if query.Location != "" {
		return "location:" + strings.ToLower(strings.TrimSpace(query.Location))
	}
	if query.Latitude == nil || query.Longitude == nil {
		return ""
	}
	return "coordinates:" + strconv.FormatFloat(*query.Latitude, 'f', -1, 64) + ":" + strconv.FormatFloat(*query.Longitude, 'f', -1, 64)
}

func (s *Service) observeCache(result string) {
	if s.observer != nil {
		s.observer.CacheLookup(result)
	}
}

func (s *Service) observeCacheStats() {
	if s.observer != nil && s.cache != nil {
		stats := s.cache.Stats()
		s.observer.CacheStats(stats.Entries, stats.Evictions)
	}
}

func retry[T any](ctx context.Context, operation string, maxAttempts int, retryDelay func(int) time.Duration, now func() time.Time, observer Observer, call func(context.Context) (T, error)) (T, error) {
	var zero T
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		attemptContext, span := otel.Tracer("weatherlookup/vendor").Start(ctx, "vendor."+operation,
			trace.WithAttributes(
				attribute.String("weatherlookup.vendor.operation", operation),
				attribute.Int("weatherlookup.vendor.attempt", attempt),
			))
		started := now()
		value, err := call(attemptContext)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "vendor request failed")
		}
		span.End()
		if observer != nil && !errors.Is(err, ErrCircuitOpen) {
			observer.VendorRequest(operation, vendorOutcome(err), now().Sub(started))
			if attempt > 1 {
				observer.VendorRetry(operation)
			}
		}
		if err == nil {
			return value, nil
		}
		if attempt == maxAttempts || !retryable(err) || ctx.Err() != nil {
			return zero, err
		}
		if err := wait(ctx, retryDelay(attempt)); err != nil {
			return zero, err
		}
	}
	return zero, context.Canceled
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func defaultRetryDelay(attempt int) time.Duration {
	base := 100 * time.Millisecond
	for index := 1; index < attempt; index++ {
		base *= 2
	}
	if base > time.Second {
		base = time.Second
	}
	return base + time.Duration(rand.Int64N(int64(base/2)+1))
}

func retryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var classified interface{ Retryable() bool }
	if errors.As(err, &classified) {
		return classified.Retryable()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return true
	}
	// Unknown, configuration, and decoding errors fail closed. Only explicitly
	// selected connection failures are retried in addition to classified errors.
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE)
}

func vendorOutcome(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var classified interface{ StatusCodeValue() int }
	if errors.As(err, &classified) {
		status := classified.StatusCodeValue()
		if status >= 500 {
			return "http_5xx"
		}
		if status >= 400 {
			return "http_4xx"
		}
	}
	return "error"
}

func conditionForCode(code int) string {
	switch {
	case code == 0:
		return "clear sky"
	case code >= 1 && code <= 3:
		return "partly cloudy"
	case code == 45 || code == 48:
		return "fog"
	case code >= 51 && code <= 57:
		return "drizzle"
	case code >= 61 && code <= 67:
		return "rain"
	case code >= 71 && code <= 77:
		return "snow"
	case code >= 80 && code <= 82:
		return "rain showers"
	case code == 85 || code == 86:
		return "snow showers"
	case code == 95:
		return "thunderstorm"
	case code == 96 || code == 99:
		return "thunderstorm with hail"
	default:
		return fmt.Sprintf("weather code %d", code)
	}
}
