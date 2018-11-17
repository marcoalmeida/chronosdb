package chronos

import (
	"time"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"go.uber.org/zap"
)

//// if the request is either an entry from an intent log being replayed or part of the cross-check process,
//// put that key in recovery mode (add/update the timestamp for the key in question)
//func (c *Chronos) checkAndSetRecoveryMode(headers http.Header) {
//	if c.request.RequestIsIntentLog(headers) || c.request.RequestIsCrosscheck(headers) {
//		// both hinted hand offs and key transfers include the key name in the query string, so this is safe
//		key := c.request.GetKeyFromHeader(headers)
//		// TODO: key might be nil
//		c.logger.Info("Putting key in recovery mode", zap.String("key", key.String()))
//		c.recoveryLock.Lock()
//		c.recovering[key] = time.Now()
//		c.recoveryLock.Unlock()
//	}
//}

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

// return true iff the given key is being replayed
// if key is nil, return true iff any key is being replayed
func (c *Chronos) isRecovering(key *coretypes.Key) bool {
	// return true iff any key is being recovered
	if key == nil {
		c.recoveryLock.RLock()
		n := len(c.recovering)
		c.recoveryLock.RUnlock()

		return n != 0
	}

	c.recoveryLock.RLock()
	_, ok := c.recovering[key]
	c.recoveryLock.RUnlock()

	return ok
}

// run in the background, continuously checking for each key's latest recovery timestamp
// exit recovery mode if RecoveryGracePeriod seconds or more have passed
func (c *Chronos) checkAndExitRecovery() {
	c.logger.Info("Starting background task for checking and exiting recovery status")

	for {
		done := make([]*coretypes.Key, 0)

		// collect all keys that were being recovered but enough time has passed since the last time we received an
		// update and assume there is nothing more to receive
		c.recoveryLock.RLock()
		for k, t := range c.recovering {
			if time.Since(t) >= (time.Second * time.Duration(c.cfg.RecoveryGracePeriod)) {
				done = append(done, k)
			}
		}
		c.recoveryLock.RUnlock()

		// remove keys that are no longer being replayed from the map
		for _, k := range done {
			c.logger.Debug("Exiting recovery", zap.String("key", k.String()))
			c.recoveryLock.Lock()
			delete(c.recovering, k)
			c.recoveryLock.Unlock()
		}

		c.logger.Debug("Sleeping between recovery status checks", zap.Int("time", c.cfg.RecoveryGracePeriod))
		time.Sleep(time.Second * time.Duration(c.cfg.RecoveryGracePeriod))
	}
}
