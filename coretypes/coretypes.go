package coretypes

import (
	"fmt"
	"strings"
)

const delimiter string = "-"

type Key struct {
	DB          string `json:"database"`
	Measurement string `json:"measurement"`
}

func NewKey(db string, measurement string) *Key {
	return &Key{
		DB:          db,
		Measurement: measurement,
	}
}

func (k *Key) String() string {
	return fmt.Sprintf("%v%s%v", k.DB, delimiter, k.Measurement)
}

func KeyFromString(k string) *Key {
	parts := strings.Split(k, delimiter)
	return &Key{
		DB:          parts[0],
		Measurement: parts[1],
	}
}
