package chronos

import (
	"net/url"
	"testing"

	"github.com/marcoalmeida/chronosdb/coretypes"
)

func TestChronos_getKeyFromURL(t *testing.T) {
	v := url.Values{}

	// no key
	k := getKeyFromURL(v)
	if k != nil {
		t.Error("Expected nil, got:", k)
	}

	// some key
	db := "db"
	measurement := "m"
	key := coretypes.NewKey(db, measurement)
	v.Add("key", key.String())
	// extract it
	k = getKeyFromURL(v)
	if k.DB != key.DB || k.Measurement != key.Measurement {
		t.Error("Expected", key.String(), "got:", k.String())
	}
}
