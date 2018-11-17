package shared

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestMin(t *testing.T) {
	if Min(-1, -4) != -4 {
		t.Error("Failed -4")
	}

	if Min(-1, 0) != -1 {
		t.Error("Failed -1")
	}

	if Min(3, 1) != 1 {
		t.Error("Failed 1")
	}

	if Min(3, 6) != 3 {
		t.Error("Failed 3")
	}

	if Min(4, 4) != 4 {
		t.Error("Failed 4")
	}
}

func TestBackoff(t *testing.T) {
	logger := zap.NewNop()

	for i := 0; i < 3; i++ {
		start := time.Now()
		Backoff(0, "", logger)
		wait := time.Since(start).Nanoseconds() / int64(time.Millisecond)

		if !(wait >= 0 && wait < int64((i+1)*100)) {
			t.Error("Expected sleep for [0 --", (i+1)*100, ") got", wait)
		}
	}
}

func TestEnsureDirectory(t *testing.T) {

}
