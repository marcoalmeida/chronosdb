package dynamo

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"go.uber.org/zap"
)

const keyRecvExt = "transferring"

var recvMarkerDirectory = fmt.Sprintf("%s/recv", DefaultDataDirectory)

// return the path to the file we use to mark am in progress key transfer
func keyRecvMarkerPath(key *coretypes.Key) string {
	return fmt.Sprintf("%s/%s.%s", recvMarkerDirectory, key.String(), keyRecvExt)
}

// create a file (if does not already exist) to signal that a key transfer has started
// this file will persist until the transfer is completed so that we never acknowledge a key as present with only
// partial data
func (dyn *Dynamo) beginKeyRecv(key *coretypes.Key) error {
	marker := keyRecvMarkerPath(key)
	dyn.logger.Debug("Begin key recv", zap.String("key", key.String()), zap.String("marker", marker))
	if _, err := os.Stat(marker); os.IsNotExist(err) {
		err := os.MkdirAll(marker, 0700)
		if err != nil {
			return err
		}
	}

	return nil
}

func (dyn *Dynamo) endKeyRecv(key *coretypes.Key) error {
	dyn.keyRecvLock.Lock()
	delete(dyn.keyRecvTimestamp, key)
	dyn.keyRecvLock.Unlock()
	return os.Remove(keyRecvMarkerPath(key))
}

// return true iff a key is being transferred
func (dyn *Dynamo) keyRecvInProgress(key *coretypes.Key) bool {
	// if there is no timestamp we know a transfer is not in progress regardless of anything else
	//
	// if there is a timestamp for the key but the last update was longer than X seconds ago, assume the source node
	// stopped sending data and the transfer is not ongoing anymore
	dyn.keyRecvLock.RLock()
	ts, ok := dyn.keyRecvTimestamp[key]
	dyn.keyRecvLock.RUnlock()

	if !ok {
		dyn.logger.Debug("Key not found in timestamp map", zap.String("key", key.String()))
		return false
	}

	ds := time.Since(ts).Seconds()
	if ds > float64(dyn.cfg.KeyRecvTimeout) {
		dyn.logger.Debug("Key recv stale", zap.String("key", key.String()), zap.Float64("elapsed", ds))
		return false
	}

	return true
}

// return true if there's a pending key transfer, regardless of whether or not it is in progress
func (dyn *Dynamo) keyRecvPending(key *coretypes.Key) bool {
	if key == nil {
		// if no key is specified return true if any key transfer if pending
		files, err := ioutil.ReadDir(recvMarkerDirectory)
		if err != nil {
			// if we can't read the directory ChronosDB won't find any references to pending recv operations
			return false
		}

		for _, file := range files {
			if filepath.Ext(file.Name()) == keyRecvExt {
				return true
			}
		}

		return false
	} else {
		_, err := os.Stat(keyRecvMarkerPath(key))
		if err != nil {
			return !os.IsNotExist(err)
		}

		return true
	}
}
