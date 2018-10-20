package mapsort

import (
	"testing"
)

func TestByValue(t *testing.T) {
	m := []string{
		"Python", "Python", "Python",
		"Golang",
		"Py", "Py"}
	count := make(map[string]int)

	for _, v := range m {
		count[v]++
	}

	// descending
	for i, k := range ByValue(count, false) {
		switch i {
		case 0:
			if !(k == "Python" && count[k] == 3) {
				t.Error("Expected (Python, 3), got", k, count[k])
			}
		case 1:
			if !(k == "Py" && count[k] == 2) {
				t.Error("Expected (Py, 2), got", k, count[k])
			}
		case 2:
			if !(k == "Golang" && count[k] == 1) {
				t.Error("Expected (Golang, 1), got", k, count[k])
			}
		}
	}

	// ascending
	for i, k := range ByValue(count, true) {
		switch i {
		case 0:
			if !(k == "Golang" && count[k] == 1) {
				t.Error("Expected (Golang, 1), got", k, count[k])
			}
		case 1:
			if !(k == "Py" && count[k] == 2) {
				t.Error("Expected (Py, 2), got", k, count[k])
			}
		case 2:
			if !(k == "Python" && count[k] == 3) {
				t.Error("Expected (Python, 3), got", k, count[k])
			}
		}
	}
}
