package coretypes

import (
	"fmt"
	"strings"
)

const keyDelimiter string = "-"

type Key struct {
	DB          string `json:"database"`
	Measurement string `json:"measurement"`
}

func (k *Key) String() string {
	if k == nil {
		return ""
	}

	return fmt.Sprintf("%v%s%v", k.DB, keyDelimiter, k.Measurement)
}

func NewKey(db string, measurement string) *Key {
	// we need both the DB and the measurement to form a key
	if db == "" || measurement == "" {
		return nil
	}

	return &Key{
		DB:          db,
		Measurement: measurement,
	}
}

func KeyFromString(k string) *Key {
	parts := strings.Split(k, keyDelimiter)
	if len(parts) != 2 {
		return nil
	}

	return &Key{
		DB:          parts[0],
		Measurement: parts[1],
	}
}
