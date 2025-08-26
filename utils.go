package flink

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// mergeProperties parses the gatewayUrl, validates required fields, and merges
// driverProperties with URL query parameters. Duplicate keys are not allowed.
// Returns the merged map[string]string and error.
func mergeProperties(gatewayUrl string, driverProperties map[string]string) (map[string]string, error) {
	uri, err := url.Parse(gatewayUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid Flink SQL Gateway URL: %s: %w", gatewayUrl, err)
	}
	if strings.TrimSpace(uri.Hostname()) == "" {
		return nil, fmt.Errorf("no host specified in uri: %s", gatewayUrl)
	}
	if uri.Port() == "" {
		return nil, fmt.Errorf("no port specified in uri: %s", gatewayUrl)
	}
	_, err = strconv.Atoi(uri.Port())
	if err != nil {
		return nil, fmt.Errorf("invalid port: %s", uri.Port())
	}
	urlProps, err := parseURIParameters(uri.RawQuery)
	if err != nil {
		return nil, err
	}
	for k := range urlProps {
		if _, dup := driverProperties[k]; dup {
			return nil, fmt.Errorf("connection property '%s' is both in the URL and an argument", k)
		}
	}
	props := map[string]string{}
	addAll(props, urlProps)
	addAll(props, driverProperties)
	return props, nil
}

func parseURIParameters(rawQuery string) (map[string]string, error) {
	out := map[string]string{}
	if rawQuery == "" {
		return out, nil
	}
	for _, pair := range strings.Split(rawQuery, "&") {
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("connection property in uri must be key=val format: '%s'", pair)
		}
		k, v := kv[0], kv[1]
		if _, exists := out[k]; exists {
			return nil, fmt.Errorf("connection property '%s' is in URL multiple times", k)
		}
		out[k] = v
	}
	return out, nil
}

func addAll(dst, src map[string]string) {
	for k, v := range src {
		dst[k] = v
	}
}
