package coretypes

import (
	"fmt"
	"testing"
)

func TestNewKey(t *testing.T) {
	k1 := NewKey("db", "m1")

	if k1.DB != "db" {
		t.Error("Expected db, got:", k1.DB)
	}

	if k1.Measurement != "m1" {
		t.Error("Expected m1, got:", k1.Measurement)
	}

	k2 := NewKey("", "foo")
	if k2 != nil {
		t.Error("Expected nil, got", k2)
	}

	if k2.String() != "" {
		t.Error("Expected an empty string, got", k2.String())
	}
}

func TestKeyFromString(t *testing.T) {
	k := KeyFromString("db0-m0")
	if k.DB != "db0" {
		t.Error("Expected db0, got:", k.DB)
	}
	if k.Measurement != "m0" {
		t.Error("Expected m0, got:", k.Measurement)
	}

	for _, badKey := range []string{"", "foo"} {
		k = KeyFromString(badKey)
		if k != nil {
			t.Error("Expected nil, got:", k)
		}
	}
}

func TestKey_String(t *testing.T) {
	k0 := NewKey("db", "m0")
	expect := fmt.Sprintf("db%sm0", keyDelimiter)
	if k0.String() != expect {
		t.Error("Expected", expect, "got:", k0.String())
	}

	k1 := KeyFromString("db1-m1")
	expect = fmt.Sprintf("db1%sm1", keyDelimiter)
	if k1.String() != expect {
		t.Error("Expected", expect, "got:", k1.String())
	}
}
