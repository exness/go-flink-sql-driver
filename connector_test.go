package flink

import (
	"net/http"
	"testing"
)

func TestConnector_NewConnector_AppliesOptions(t *testing.T) {
	url := "http://example.com"
	httpClient := &http.Client{}
	apiVersion := "v4"
	raw, err := NewConnector(
		WithGatewayURL(url),
		WithClient(httpClient),
		WithAPIVersion(apiVersion),
	)
	c := raw.(*connector)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if c.config.GatewayURL != url {
		t.Fatalf("expected GatewayURL %q, got %q", url, c.config.GatewayURL)
	}
	if c.config.Client != httpClient {
		t.Fatalf("expected Client %v, got %v", httpClient, c.config.Client)
	}
	if c.config.APIVersion != apiVersion {
		t.Fatalf("expected APIVersion %q, got %q", apiVersion, c.config.APIVersion)
	}
}

func TestConnector_WithProperties(t *testing.T) {
	props1 := map[string]string{
		"a": "1",
		"b": "2",
	}
	props2 := map[string]string{
		"b": "3",
		"c": "4",
	}
	raw, err := NewConnector(WithProperties(props1), WithProperties(props2))
	c := raw.(*connector)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	want := map[string]string{
		"a": "1",
		"b": "3", // props2 should override props1
		"c": "4",
	}
	for k, v := range want {
		if c.config.Properties[k] != v {
			t.Errorf("expected property %q=%q, got %q", k, v, c.config.Properties[k])
		}
	}
	for k := range c.config.Properties {
		if _, ok := want[k]; !ok {
			t.Errorf("unexpected property %q in connector config", k)
		}
	}
}
