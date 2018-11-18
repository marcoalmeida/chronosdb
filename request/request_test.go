package request

import (
	"net/http"
	"testing"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"go.uber.org/zap"
)

const (
	testNode       = "node"
	testPort       = 42
	testTimeout    = 1
	testMaxRetries = 1
)

var logger = zap.NewNop()

func TestRequest_GenerateForwardURL(t *testing.T) {
	r := New(testNode, testPort, testTimeout, testTimeout, testMaxRetries, logger)

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
	r := New(testNode, testPort, testTimeout, testTimeout, testMaxRetries, logger)

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
	r := New(testNode, testPort, testTimeout, testTimeout, testMaxRetries, logger)

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
	r := New(testNode, testPort, testTimeout, testTimeout, testMaxRetries, logger)

	headers := http.Header{}
	r.SetCrosscheckHeaders(nil, &headers)
	if headers.Get(headerXCrosscheck) != "true" {
		t.Error("Expected a cross-check header")
	}
	// there should be a header for the key with an empty string value
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
	r := New(testNode, testPort, testTimeout, testTimeout, testMaxRetries, logger)

	headers := http.Header{}

	if !r.NodeIsCoordinator(headers) {
		t.Error("Unexpectedly found forward header:", r.NodeIsCoordinator(headers))
	}

	r.SetForwardHeaders(nil, &headers)
	if r.NodeIsCoordinator(headers) {
		t.Error("Expected to find forward header:", r.NodeIsCoordinator(headers))
	}
}

func TestRequest_SetIntentLogHeaders(t *testing.T) {
	r := New(testNode, testPort, testTimeout, testTimeout, testMaxRetries, logger)

	headers := http.Header{}
	r.SetIntentLogHeaders(nil, &headers)
	if headers.Get(headerXIntentLog) != "true" {
		t.Error("Expected an intent log header")
	}
	// there should be a header for the key with an empty string value
	if headers.Get(headerXKey) != "" {
		t.Error("Expected a header for the key with an empty value, got", headers.Get(headerXKey))
	}

	// now with a valid key
	k := coretypes.NewKey("db", "m")
	r.SetForwardHeaders(k, &headers)
	if headers.Get(headerXIntentLog) != "true" {
		t.Error("Expected an intent log header")
	}
	if headers.Get(headerXKey) != k.String() {
		t.Error("Expected a header with key", k.String(), "got", headers.Get(headerXKey))
	}
}

func TestRequest_RequestIsIntentLog(t *testing.T) {
	r := New(testNode, testPort, testTimeout, testTimeout, testMaxRetries, logger)

	headers := http.Header{}

	if r.RequestIsIntentLog(headers) {
		t.Error("Unexpectedly found intent log header, got", r.RequestIsIntentLog(headers))
	}

	r.SetForwardHeaders(nil, &headers)
	if r.RequestIsIntentLog(headers) {
		t.Error("Expected to find intent log header, got", r.RequestIsIntentLog(headers))
	}
}

func TestRequest_GetKeyFromRequest(t *testing.T) {
	r := New(testNode, testPort, testTimeout, testTimeout, testMaxRetries, logger)

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
