package chronos

import (
	"time"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"go.uber.org/zap"
)

// put the given key in recovery mode or update the
func (c *Chronos) setRecovering(key *coretypes.Key) {
	if key == nil {
		c.logger.Error("Attempted to put a nil key in recovery mode")
		return
	}

	c.recoveryLock.RLock()
	c.recovering[key] = time.Now()
	c.recoveryLock.RUnlock()
}

// return true iff the given key is being recovered
func (c *Chronos) isRecovering(key *coretypes.Key) bool {
	// don't even try to access the map
	if key == nil {
		c.logger.Error("Attempted to check if a nil key is in recovery mode")
		return false
	}

	c.recoveryLock.RLock()
	_, ok := c.recovering[key]
	c.recoveryLock.RUnlock()

	return ok
}

// run in the background, continuously checking for each key's latest recovery timestamp
// exit recovery mode if RecoveryGracePeriod seconds or more have passed
func (c *Chronos) checkAndExitRecovery() {
	c.logger.Info("Starting background task for exiting recovery mode")

	for {
		done := make([]*coretypes.Key, 0)

		c.recoveryLock.RLock()
		for k, t := range c.recovering {
			if time.Since(t) >= (time.Second * time.Duration(c.cfg.RecoveryGracePeriod)) {
				done = append(done, k)
			}
		}
		c.recoveryLock.RUnlock()

		for _, k := range done {
			c.logger.Debug("Exiting recovery mode", zap.String("key", k.String()))
			c.recoveryLock.Lock()
			delete(c.recovering, k)
			c.recoveryLock.Unlock()
		}

		c.logger.Debug("Sleeping for checking recovery mode", zap.Int("time", c.cfg.RecoveryGracePeriod))
		time.Sleep(time.Second * time.Duration(c.cfg.RecoveryGracePeriod))
	}
}
