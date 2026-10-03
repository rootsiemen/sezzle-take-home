package openmeteo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestClientCurrentWeather(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/forecast" {
			t.Fatalf("path = %q, want /v1/forecast", request.URL.Path)
		}
		query := request.URL.Query()
		if query.Get("latitude") != "44.98" || query.Get("longitude") != "-93.27" {
			t.Fatalf("coordinates = %q, %q", query.Get("latitude"), query.Get("longitude"))
		}
		if query.Get("timezone") != "auto" || query.Get("temperature_unit") != "celsius" {
			t.Fatalf("missing explicit unit/timezone query: %v", query)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"latitude":44.98,"longitude":-93.27,"timezone":"America/Chicago","current":{"time":"2026-09-15T12:00","temperature_2m":18.4,"relative_humidity_2m":61,"apparent_temperature":18.1,"is_day":1,"precipitation":0,"rain":0,"showers":0,"snowfall":0,"weather_code":2,"cloud_cover":44,"wind_speed_10m":13.2,"wind_direction_10m":180}}`))
	}))
	defer server.Close()

	client := NewClient(Config{BaseURL: server.URL, GeocodingBaseURL: server.URL})
	location, current, err := client.CurrentWeather(context.Background(), 44.98, -93.27)
	if err != nil {
		t.Fatalf("CurrentWeather() error = %v", err)
	}
	if location.Timezone != "America/Chicago" || location.Latitude != 44.98 {
		t.Fatalf("location = %+v", location)
	}
	if current.TemperatureC != 18.4 || current.WeatherCode != 2 || !current.IsDay {
		t.Fatalf("current = %+v", current)
	}
}

func TestClientSearch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/search" {
			t.Fatalf("path = %q, want /v1/search", request.URL.Path)
		}
		if request.URL.Query().Get("name") != "São Paulo" {
			t.Fatalf("name = %q", request.URL.Query().Get("name"))
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"results":[{"name":"São Paulo","latitude":-23.55,"longitude":-46.63,"timezone":"America/Sao_Paulo","country":"Brazil","country_code":"BR","admin1":"São Paulo"}]}`))
	}))
	defer server.Close()

	client := NewClient(Config{BaseURL: server.URL, GeocodingBaseURL: server.URL})
	location, err := client.Search(context.Background(), " São Paulo ")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if location.Name != "São Paulo" || location.Latitude != -23.55 || location.CountryCode != "BR" {
		t.Fatalf("location = %+v", location)
	}
}

func TestClientSearchNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client := NewClient(Config{BaseURL: server.URL, GeocodingBaseURL: server.URL})
	_, err := client.Search(context.Background(), "does-not-exist")
	if !errors.Is(err, ErrLocationNotFound) {
		t.Fatalf("Search() error = %v, want ErrLocationNotFound", err)
	}
}

func TestClientHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusTooManyRequests)
		_, _ = response.Write([]byte("rate limited"))
	}))
	defer server.Close()

	client := NewClient(Config{BaseURL: server.URL, GeocodingBaseURL: server.URL})
	_, _, err := client.CurrentWeather(context.Background(), 1, 2)
	var httpError *HTTPError
	if !errors.As(err, &httpError) || httpError.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("error = %v, want HTTPError 429", err)
	}
}

func TestClientEndpointPreservesBasePath(t *testing.T) {
	client := NewClient(Config{BaseURL: "https://example.com/api/"})
	endpoint, err := client.endpoint(client.baseURL, "/v1/search")
	if err != nil {
		t.Fatal(err)
	}
	want := "https://example.com/api/v1/search"
	if endpoint.String() != want {
		t.Fatalf("endpoint = %q, want %q", endpoint.String(), want)
	}
	_, _ = url.Parse(want)
}
