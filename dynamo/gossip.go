package dynamo

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"github.com/marcoalmeida/chronosdb/responsetypes"
	"github.com/marcoalmeida/chronosdb/shared"
)

func (dyn *Dynamo) askHasKey(node string, key *coretypes.Key) (bool, error) {
	status, response := shared.DoGet(
		fmt.Sprintf("http://%s:%d/key/%s", node, dyn.chronosDBPort, key.String()),
		nil,
		dyn.httpClient,
		dyn.cfg.MaxRetries,
		dyn.logger,
		"dynamo.askHasKey",
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

func (dyn *Dynamo) tellHasKey(node string, key *coretypes.Key) error {
	status, response := shared.DoPut(
		fmt.Sprintf("http://%s:%d/key/%s", node, dyn.chronosDBPort, key.String()),
		nil,
		nil,
		dyn.httpClient,
		dyn.cfg.MaxRetries,
		dyn.logger,
		"dynamo.tellHasKey",
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
