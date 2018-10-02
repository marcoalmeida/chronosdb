package coretypes

import (
	"fmt"
	"testing"
)

// TODO: replace - with the const delimiter

func Test_GenerateKey(t *testing.T) {
	// all new
	k1 := NewKey("db", "m1")
	if k1.String() != fmt.Sprintf("db%sm1", delimiter) {
		t.Error("Expected dbm1, got:", k1)
	}

	// db cached, new measurement
	k2 := NewKey("db", "m2")
	if k2.String() != fmt.Sprintf("db%sm2", delimiter) {
		t.Error("Expected dbm2, got:", k2)
	}

	// all cached
	k3 := NewKey("db", "m1")
	if k3.String() != fmt.Sprintf("db%sm1", delimiter) {
		t.Error("Expected dbm1, got:", k3)
	}
}
