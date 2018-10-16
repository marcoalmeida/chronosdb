package ilog

import (
	"os"
	"testing"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"go.uber.org/zap"
)

const (
	node        = "node0"
	db          = "db0"
	uri         = "localhost:8989/write?db=db0"
	measurement = "m"
	dataDir     = "datadir"
)

var payload []byte = []byte("Action,action=user/create,cluster=ec2,environment=development count=1i 1492560394223581000")

func cleanup(t *testing.T) {
	// make sure there are no lost older tests
	err := os.RemoveAll(dataDir)
	if err != nil {
		t.Error(err)
	}
}

func storeHint(t *testing.T) error {
	logger, err := zap.NewProduction()
	if err != nil {
		t.Error(err)
	}

	h := New(dataDir, logger)
	c := &Entry{
		Node: node,
		URI:  uri,
		Key:  coretypes.NewKey(db, measurement),
		//DB:          db,
		//Measurement: measurement,
		Payload: payload,
	}

	return h.Add(c)
}

func TestCfg_Store(t *testing.T) {
	err := storeHint(t)
	if err != nil {
		t.Error(err)
	}

	cleanup(t)
}

func TestCfg_Fetch(t *testing.T) {
	// make sure a hint exists
	storeHint(t)

	logger, err := zap.NewProduction()
	if err != nil {
		t.Error(err)
	}

	h := New(dataDir, logger)
	c, err := h.Fetch()
	if err != nil {
		t.Error(err)
	}

	if c.Node != node {
		t.Error("Expected", node, "got", c.Node)
	}

	//if c.DB != db {
	//	t.Error("Expected", db, "got", c.DB)
	//}

	if c.URI != uri {
		t.Error("Expected", uri, "got", c.URI)
	}

	if c.Key.String() != coretypes.NewKey(db, measurement).String() {
		t.Error("Expected", coretypes.NewKey(db, measurement).String(), "got", c.Key.String())
	}

	if string(c.Payload) != string(payload) {
		t.Error("Expected", payload, "got", c.Payload)
	}

	cleanup(t)
}
