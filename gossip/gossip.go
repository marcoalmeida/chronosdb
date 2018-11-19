package gossip

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"github.com/marcoalmeida/chronosdb/responsetypes"
	"github.com/marcoalmeida/chronosdb/shared"
	"go.uber.org/zap"
)

type Gossip struct {
	chronosDBPort int64
	logger        *zap.Logger
	httpClient    *http.Client
	maxRetries    int
}

func New(chronosDBPort int64, httpClient *http.Client, maxRetries int, logger *zap.Logger) *Gossip {
	return &Gossip{
		chronosDBPort: chronosDBPort,
		logger:        logger,
		httpClient:    httpClient,
		maxRetries:    maxRetries,
	}
}

// AskHasKey returns true iff a given node already has a given key
func (g *Gossip) AskHasKey(node string, key *coretypes.Key) (bool, error) {
	status, response := shared.DoGet(
		fmt.Sprintf("http://%s:%d/key/%s", node, g.chronosDBPort, key.String()),
		nil,
		g.httpClient,
		g.maxRetries,
		g.logger,
	)

	switch status {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		e := responsetypes.Error{}
		err := json.Unmarshal(response, &e)
		if err != nil {
			return false, err
		}

		return false, errors.New(e.Message)
	}
}

// TellHasKey notifies a given node that all data relative to key has been transferred
func (g *Gossip) TellHasKey(node string, key *coretypes.Key) error {
	status, response := shared.DoPut(
		fmt.Sprintf("http://%s:%d/key/%s", node, g.chronosDBPort, key.String()),
		nil,
		nil,
		g.httpClient,
		g.maxRetries,
		g.logger,
	)

	switch status {
	case http.StatusOK:
		return nil
	case http.StatusNotFound:
		return nil
	default:
		error := responsetypes.Error{}
		err := json.Unmarshal(response, &error)
		if err != nil {
			return err
		}

		return errors.New(error.Message)
	}
}

func (g *Gossip) GetDBs(node string) (*responsetypes.GetDBs, error) {
	status, response := shared.DoGet(
		fmt.Sprintf("http://%s:%d/db/", node, g.chronosDBPort),
		nil,
		g.httpClient,
		g.maxRetries,
		g.logger,
	)
	if !(status >= 200 && status <= 299) {
		return nil, errors.New(string(response))
	}

	dbs := &responsetypes.GetDBs{}
	err := json.Unmarshal(response, &dbs)
	if err != nil {
		return nil, err
	}

	return dbs, nil
}
