package chronos

import (
	"net/http"
	"testing"

	"github.com/marcoalmeida/chronosdb/coretypes"
)

func TestChronos_forwardHeaders(t *testing.T) {
	headers := http.Header{}

	if !nodeIsCoordinator(headers) {
		t.Error("Unexpectedly found forward header:", nodeIsCoordinator(headers))
	}

	setForwardHeaders(nil, &headers)
	if nodeIsCoordinator(headers) {
		t.Error("Expected to find forward header:", nodeIsCoordinator(headers))
	}
}

func TestChronos_getKeyFromRequest(t *testing.T) {
	headers := http.Header{}

	k0 := getKeyFromRequest(headers)
	if k0 != nil {
		t.Error("Expected nil key, got", k0)
	}

	// use setForwardHeaders to set a key
	k1 := coretypes.NewKey("db", "m")
	setForwardHeaders(k1, &headers)
	k2 := getKeyFromRequest(headers)
	if k2.String() != k1.String() {
		t.Error("Expected", k2, "got", k2)
	}
}
