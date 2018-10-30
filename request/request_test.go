package request

import (
	"net/http"
	"testing"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"go.uber.org/zap"
)

const testPort = 42

var logger = zap.NewNop()

func TestRequest_GenerateForwardURL(t *testing.T) {
	r := New(testPort, logger)

	node := "chronos.host"
	uri := []string{"", "/write?db=db"}
	expect := []string{"http://chronos.host:42/", "http://chronos.host:42/write?db=db"}

	for i := 0; i < len(uri); i++ {
		u := r.GenerateForwardURL(node, uri[i])
		if u != expect[i] {
			t.Error("Expected", expect[i], "got", u)
		}
	}

	// no node --> no URL
	u := r.GenerateForwardURL("", uri[0])
	if u != "" {
		t.Error("Expected empty URL got", u)
	}
}

func TestRequest_SetForwardHeaders(t *testing.T) {
	r := New(testPort, logger)

	headers := http.Header{}
	r.SetForwardHeaders(nil, &headers)
	if headers.Get(headerXForward) != "true" {
		t.Error("Expected a forward header")
	}

	// now with some key
	k := coretypes.NewKey("db", "m")
	r.SetForwardHeaders(k, &headers)
	if headers.Get(headerXForward) != "true" {
		t.Error("Expected a forward header")
	}
	if headers.Get(headerXKey) != k.String() {
		t.Error("Expected a header with key", k.String(), "got", headers.Get(headerXKey))
	}
}

func TestRequest_NodeIsCoordinator(t *testing.T) {
	r := New(testPort, logger)

	headers := http.Header{}

	if !r.NodeIsCoordinator(headers) {
		t.Error("Unexpectedly found forward header:", r.NodeIsCoordinator(headers))
	}

	r.SetForwardHeaders(nil, &headers)
	if r.NodeIsCoordinator(headers) {
		t.Error("Expected to find forward header:", r.NodeIsCoordinator(headers))
	}
}

func TestRequest_SetCrosscheckHeaders(t *testing.T) {
	r := New(testPort, logger)

	headers := http.Header{}
	r.SetCrosscheckHeaders(nil, &headers)
	if headers.Get(headerXCrosscheck) != "true" {
		t.Error("Expected a cross-check header")
	}
	// there should be a header for the key, with and empty string value
	if headers.Get(headerXKey) != "" {
		t.Error("Expected a header for the key with an empty value, got", headers.Get(headerXKey))
	}

	// now with a valid key
	k := coretypes.NewKey("db", "m")
	r.SetForwardHeaders(k, &headers)
	if headers.Get(headerXCrosscheck) != "true" {
		t.Error("Expected a cross-check header")
	}
	if headers.Get(headerXKey) != k.String() {
		t.Error("Expected a header with key", k.String(), "got", headers.Get(headerXKey))
	}
}

func TestRequest_RequestIsCrosscheck(t *testing.T) {
	r := New(testPort, logger)

	headers := http.Header{}

	if !r.NodeIsCoordinator(headers) {
		t.Error("Unexpectedly found forward header:", r.NodeIsCoordinator(headers))
	}

	r.SetForwardHeaders(nil, &headers)
	if r.NodeIsCoordinator(headers) {
		t.Error("Expected to find forward header:", r.NodeIsCoordinator(headers))
	}
}

// TODO: test the intent log functions

func TestRequest_GetKeyFromRequest(t *testing.T) {
	r := New(testPort, logger)

	headers := http.Header{}

	k0 := r.GetKeyFromHeader(headers)
	if k0 != nil {
		t.Error("Expected nil key, got", k0)
	}

	// use setForwardHeaders to set a key
	k1 := coretypes.NewKey("db", "m")
	r.SetForwardHeaders(k1, &headers)
	k2 := r.GetKeyFromHeader(headers)
	if k2.String() != k1.String() {
		t.Error("Expected", k2, "got", k2)
	}
}
