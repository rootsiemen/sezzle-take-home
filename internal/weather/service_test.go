package weather

import (
	"context"
	"errors"
	"testing"
	"time"

	"weatherlookup/internal/cache"
)

type fakeProvider struct {
	location Location
	current  Current
	err      error
	searches int
	lookups  int
}

func (f *fakeProvider) Search(context.Context, string) (Location, error) {
	f.searches++
	return f.location, f.err
}

func (f *fakeProvider) CurrentWeather(context.Context, float64, float64) (Location, Current, error) {
	f.lookups++
	return f.location, f.current, f.err
}

func TestServiceLookupByCoordinates(t *testing.T) {
	provider := &fakeProvider{
		location: Location{Latitude: 40.7, Longitude: -74, Timezone: "America/New_York"},
		current:  Current{Time: "2026-09-15T12:00", TemperatureC: 21.5, RelativeHumidityPct: 50, IsDay: true, WeatherCode: 61, WindSpeedKmh: 10},
	}
	service := NewService(provider)
	latitude, longitude := 40.7, -74.0

	result, err := service.Lookup(context.Background(), Query{Latitude: &latitude, Longitude: &longitude})
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if provider.searches != 0 || provider.lookups != 1 {
		t.Fatalf("provider calls = searches %d, lookups %d", provider.searches, provider.lookups)
	}
	if result.Location.Timezone != "America/New_York" || result.Current.Condition != "rain" || !result.Current.IsDay {
		t.Fatalf("result = %+v", result)
	}
}

func TestServiceLookupByLocation(t *testing.T) {
	provider := &fakeProvider{
		location: Location{Name: "London", Latitude: 51.5, Longitude: -0.12, Country: "United Kingdom"},
		current:  Current{WeatherCode: 0},
	}
	service := NewService(provider)

	result, err := service.Lookup(context.Background(), Query{Location: "London"})
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if provider.searches != 1 || provider.lookups != 1 || result.Location.Name != "London" {
		t.Fatalf("calls/result = %d/%d/%+v", provider.searches, provider.lookups, result)
	}
}

func TestServiceRejectsInvalidCoordinates(t *testing.T) {
	provider := &fakeProvider{}
	service := NewService(provider)
	latitude, longitude := 91.0, 0.0

	_, err := service.Lookup(context.Background(), Query{Latitude: &latitude, Longitude: &longitude})
	if err == nil || provider.lookups != 0 {
		t.Fatalf("Lookup() error = %v, lookups = %d", err, provider.lookups)
	}
}

func TestServicePropagatesProviderError(t *testing.T) {
	want := errors.New("provider unavailable")
	provider := &fakeProvider{err: want}
	service := NewService(provider)

	_, err := service.Lookup(context.Background(), Query{Location: "London"})
	if !errors.Is(err, want) {
		t.Fatalf("Lookup() error = %v, want %v", err, want)
	}
}

func TestServiceCachesSuccessfulResult(t *testing.T) {
	provider := &fakeProvider{
		location: Location{Latitude: 40.7, Longitude: -74, Timezone: "America/New_York"},
		current:  Current{WeatherCode: 0},
	}
	service := NewServiceWithOptions(provider, Options{
		Cache:      cache.New[Result](10, time.Minute),
		RetryDelay: func(int) time.Duration { return 0 },
	})
	latitude, longitude := 40.7, -74.0

	first, firstMetadata, err := service.LookupWithMetadata(context.Background(), Query{Latitude: &latitude, Longitude: &longitude})
	if err != nil {
		t.Fatal(err)
	}
	second, secondMetadata, err := service.LookupWithMetadata(context.Background(), Query{Latitude: &latitude, Longitude: &longitude})
	if err != nil {
		t.Fatal(err)
	}
	if first != second || firstMetadata.CacheStatus != CacheMiss || secondMetadata.CacheStatus != CacheHit {
		t.Fatalf("results/metadata = %+v/%+v and %+v/%+v", first, firstMetadata, second, secondMetadata)
	}
	if provider.lookups != 1 {
		t.Fatalf("provider lookups = %d, want 1", provider.lookups)
	}
}

type flakyProvider struct {
	currentCalls int
}

func (p *flakyProvider) Search(context.Context, string) (Location, error) {
	return Location{Latitude: 1, Longitude: 2}, nil
}

func (p *flakyProvider) CurrentWeather(context.Context, float64, float64) (Location, CurrentWeather, error) {
	p.currentCalls++
	if p.currentCalls > 1 {
		return Location{}, CurrentWeather{}, context.DeadlineExceeded
	}
	return Location{Latitude: 1, Longitude: 2, Timezone: "UTC"}, CurrentWeather{WeatherCode: 0}, nil
}

func TestServiceServesStaleOnTransientVendorFailure(t *testing.T) {
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	weatherCache := cache.NewWithClock[Result](10, 5*time.Minute, func() time.Time { return now })
	provider := &flakyProvider{}
	service := NewServiceWithOptions(provider, Options{
		Cache:       weatherCache,
		MaxAttempts: 2,
		RetryDelay:  func(int) time.Duration { return 0 },
	})
	latitude, longitude := 1.0, 2.0
	if _, _, err := service.LookupWithMetadata(context.Background(), Query{Latitude: &latitude, Longitude: &longitude}); err != nil {
		t.Fatal(err)
	}

	now = now.Add(6 * time.Minute)
	result, metadata, err := service.LookupWithMetadata(context.Background(), Query{Latitude: &latitude, Longitude: &longitude})
	if err != nil {
		t.Fatal(err)
	}
	if result.Current.Condition != "clear sky" || metadata.CacheStatus != CacheStale || metadata.Age != 6*time.Minute {
		t.Fatalf("result/metadata = %+v/%+v", result, metadata)
	}
	if provider.currentCalls != 3 {
		t.Fatalf("provider calls = %d, want initial call plus two retry attempts", provider.currentCalls)
	}
}

func TestServiceRetriesTransientVendorFailure(t *testing.T) {
	provider := &flakyOnceProvider{}
	service := NewServiceWithOptions(provider, Options{
		MaxAttempts: 2,
		RetryDelay:  func(int) time.Duration { return 0 },
	})
	latitude, longitude := 1.0, 2.0
	if _, err := service.Lookup(context.Background(), Query{Latitude: &latitude, Longitude: &longitude}); err != nil {
		t.Fatal(err)
	}
	if provider.currentCalls != 2 {
		t.Fatalf("provider calls = %d, want 2", provider.currentCalls)
	}
}

type flakyOnceProvider struct {
	currentCalls int
}

func (p *flakyOnceProvider) Search(context.Context, string) (Location, error) {
	return Location{}, nil
}

func (p *flakyOnceProvider) CurrentWeather(context.Context, float64, float64) (Location, CurrentWeather, error) {
	p.currentCalls++
	if p.currentCalls == 1 {
		return Location{}, CurrentWeather{}, context.DeadlineExceeded
	}
	return Location{Latitude: 1, Longitude: 2}, CurrentWeather{WeatherCode: 0}, nil
}
