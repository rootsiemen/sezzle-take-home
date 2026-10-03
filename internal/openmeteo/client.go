// Package openmeteo contains the adapter for the Open-Meteo HTTP APIs.
package openmeteo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"weatherlookup/internal/validation"
	"weatherlookup/internal/weather"
)

const (
	defaultBaseURL          = "https://api.open-meteo.com"
	defaultGeocodingBaseURL = "https://geocoding-api.open-meteo.com"
	maxResponseSize         = 1 << 20
	maxErrorMessageBytes    = 256
)

var ErrLocationNotFound error = locationNotFoundError{}

type locationNotFoundError struct{}

func (locationNotFoundError) Error() string   { return "location not found" }
func (locationNotFoundError) Retryable() bool { return false }

// Config controls the Open-Meteo client. HTTPClient is injectable so callers
// can set a timeout and tests can use a fake transport.
type Config struct {
	BaseURL          string
	GeocodingBaseURL string
	HTTPClient       *http.Client
}

type Client struct {
	baseURL          string
	geocodingBaseURL string
	httpClient       *http.Client
}

func NewClient(config Config) *Client {
	baseURL := strings.TrimRight(config.BaseURL, "/")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	geocodingBaseURL := strings.TrimRight(config.GeocodingBaseURL, "/")
	if geocodingBaseURL == "" {
		if config.BaseURL == "" {
			geocodingBaseURL = defaultGeocodingBaseURL
		} else {
			// A custom weather base URL is commonly a test server or proxy;
			// using it for both APIs makes that override self-contained.
			geocodingBaseURL = baseURL
		}
	}

	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &Client{baseURL: baseURL, geocodingBaseURL: geocodingBaseURL, httpClient: httpClient}
}

type geocodingResponse struct {
	Results []weather.Location `json:"results"`
}

// Search returns the best Open-Meteo geocoding match for a place name.
func (c *Client) Search(ctx context.Context, name string) (location weather.Location, err error) {
	ctx, span := otel.Tracer("weatherlookup/openmeteo").Start(ctx, "openmeteo.geocoding.search")
	span.SetAttributes(attribute.String("weatherlookup.vendor.operation", "geocoding.search"))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Open-Meteo geocoding failed")
		}
		span.End()
	}()
	name, err = validation.Location(name)
	if err != nil {
		return weather.Location{}, err
	}
	if name == "" {
		return weather.Location{}, fmt.Errorf("place name is required")
	}

	endpoint, err := c.endpoint(c.geocodingBaseURL, "/v1/search")
	if err != nil {
		return weather.Location{}, err
	}
	query := endpoint.Query()
	query.Set("name", name)
	query.Set("count", "1")
	query.Set("language", "en")
	query.Set("format", "json")
	endpoint.RawQuery = query.Encode()

	var response geocodingResponse
	if err := c.getJSON(ctx, endpoint, &response); err != nil {
		return weather.Location{}, err
	}
	if len(response.Results) == 0 {
		return weather.Location{}, ErrLocationNotFound
	}
	return response.Results[0], nil
}

type Current struct {
	Time                string  `json:"time"`
	Temperature2M       float64 `json:"temperature_2m"`
	RelativeHumidity    float64 `json:"relative_humidity_2m"`
	ApparentTemperature float64 `json:"apparent_temperature"`
	IsDay               int     `json:"is_day"`
	Precipitation       float64 `json:"precipitation"`
	Rain                float64 `json:"rain"`
	Showers             float64 `json:"showers"`
	Snowfall            float64 `json:"snowfall"`
	WeatherCode         int     `json:"weather_code"`
	CloudCover          float64 `json:"cloud_cover"`
	WindSpeed10M        float64 `json:"wind_speed_10m"`
	WindDirection10M    float64 `json:"wind_direction_10m"`
}

type forecastResponse struct {
	Latitude  float64  `json:"latitude"`
	Longitude float64  `json:"longitude"`
	Timezone  string   `json:"timezone"`
	Current   *Current `json:"current"`
}

// CurrentWeather fetches current conditions for WGS84 coordinates. Units are
// explicitly requested so this adapter's output stays stable and documented:
// Celsius, km/h, and millimetres.
func (c *Client) CurrentWeather(ctx context.Context, latitude, longitude float64) (location weather.Location, current weather.CurrentWeather, err error) {
	ctx, span := otel.Tracer("weatherlookup/openmeteo").Start(ctx, "openmeteo.forecast.current")
	span.SetAttributes(
		attribute.String("weatherlookup.vendor.operation", "forecast.current"),
		attribute.Float64("weatherlookup.location.latitude", latitude),
		attribute.Float64("weatherlookup.location.longitude", longitude),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Open-Meteo forecast failed")
		}
		span.End()
	}()
	endpoint, err := c.endpoint(c.baseURL, "/v1/forecast")
	if err != nil {
		return weather.Location{}, weather.CurrentWeather{}, err
	}
	query := endpoint.Query()
	query.Set("latitude", strconv.FormatFloat(latitude, 'f', -1, 64))
	query.Set("longitude", strconv.FormatFloat(longitude, 'f', -1, 64))
	query.Set("current", strings.Join([]string{
		"temperature_2m",
		"relative_humidity_2m",
		"apparent_temperature",
		"is_day",
		"precipitation",
		"rain",
		"showers",
		"snowfall",
		"weather_code",
		"cloud_cover",
		"wind_speed_10m",
		"wind_direction_10m",
	}, ","))
	query.Set("temperature_unit", "celsius")
	query.Set("wind_speed_unit", "kmh")
	query.Set("precipitation_unit", "mm")
	query.Set("timezone", "auto")
	endpoint.RawQuery = query.Encode()

	var response forecastResponse
	if err := c.getJSON(ctx, endpoint, &response); err != nil {
		return weather.Location{}, weather.CurrentWeather{}, err
	}
	if response.Current == nil {
		return weather.Location{}, weather.CurrentWeather{}, errors.New("Open-Meteo response did not contain current weather")
	}

	return weather.Location{
		Latitude:  response.Latitude,
		Longitude: response.Longitude,
		Timezone:  response.Timezone,
	}, normalizeCurrent(*response.Current), nil
}

func normalizeCurrent(current Current) weather.CurrentWeather {
	return weather.CurrentWeather{
		Time:                 current.Time,
		TemperatureC:         current.Temperature2M,
		RelativeHumidityPct:  current.RelativeHumidity,
		ApparentTemperatureC: current.ApparentTemperature,
		IsDay:                current.IsDay == 1,
		PrecipitationMM:      current.Precipitation,
		RainMM:               current.Rain,
		ShowersMM:            current.Showers,
		SnowfallCM:           current.Snowfall,
		WeatherCode:          current.WeatherCode,
		CloudCoverPct:        current.CloudCover,
		WindSpeedKmh:         current.WindSpeed10M,
		WindDirectionDeg:     current.WindDirection10M,
	}
}

func (c *Client) endpoint(baseURL, path string) (*url.URL, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse Open-Meteo base URL: %w", err)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("Open-Meteo base URL must use http or https")
	}
	base.Path = strings.TrimRight(base.Path, "/") + path
	base.RawQuery = ""
	return base, nil
}

func (c *Client) getJSON(ctx context.Context, endpoint *url.URL, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("create Open-Meteo request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "weatherlookup/1.0")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("call Open-Meteo: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		// Read only enough to build a bounded preview and detect truncation.
		// HTTPError is also recorded in spans, not just application logs.
		body, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorMessageBytes+1))
		message := boundedErrorMessage(string(body))
		if message == "" {
			message = http.StatusText(response.StatusCode)
		}
		return &HTTPError{StatusCode: response.StatusCode, Message: message}
	}

	decoder := json.NewDecoder(io.LimitReader(response.Body, maxResponseSize))
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode Open-Meteo response: %w", err)
	}
	return nil
}

type HTTPError struct {
	StatusCode int
	Message    string
}

// Retryable classifies vendor responses that may succeed on a later attempt.
func (e *HTTPError) Retryable() bool {
	if e == nil {
		return false
	}
	switch e.StatusCode {
	case http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// StatusCodeValue lets the service metrics classify vendor failures without
// importing this adapter package into the domain package.
func (e *HTTPError) StatusCodeValue() int {
	if e == nil {
		return 0
	}
	return e.StatusCode
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("Open-Meteo returned HTTP %d: %s", e.StatusCode, boundedErrorMessage(e.Message))
}

// boundedErrorMessage keeps vendor-controlled diagnostics small and single-line,
// even for errors constructed outside getJSON. This is not secret redaction.
func boundedErrorMessage(message string) string {
	truncated := len(message) > maxErrorMessageBytes
	if truncated {
		message = message[:maxErrorMessageBytes]
	}
	message = strings.ToValidUTF8(message, "")
	message = strings.TrimSpace(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return ' '
		}
		return r
	}, message))
	if truncated {
		const suffix = " [truncated]"
		limit := maxErrorMessageBytes - len(suffix)
		if len(message) > limit {
			for !utf8.RuneStart(message[limit]) {
				limit--
			}
			message = message[:limit]
		}
		message = strings.TrimSpace(message) + suffix
	}
	return message
}
