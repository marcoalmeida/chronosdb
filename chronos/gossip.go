package chronos

//
//import (
//	"encoding/json"
//	"errors"
//	"fmt"
//	"net/http"
//
//	"github.com/marcoalmeida/chronosdb/coretypes"
//	"github.com/marcoalmeida/chronosdb/responsetypes"
//	"github.com/marcoalmeida/chronosdb/shared"
//)
//
//func (d *Chronos) askHasKey(node string, key *coretypes.Key) (bool, error) {
//	status, response := shared.DoGet(
//		fmt.Sprintf("http://%s:%d/key/%s", node, d.chronosDBPort, key.String()),
//		nil,
//		d.httpClient,
//		d.cfg.MaxRetries,
//		d.logger,
//		"dynamo.askHasKey",
//	)
//
//	switch status {
//	case http.StatusOK:
//		return true, nil
//	case http.StatusNotFound:
//		return false, nil
//	default:
//		e := responsetypes.Error{}
//		err := json.Unmarshal(response, &e)
//		if err != nil {
//			return false, err
//		}
//
//		return false, errors.New(e.Message)
//	}
//}
//
//func (d *Chronos) tellHasKey(node string, key *coretypes.Key) error {
//	status, response := shared.DoPut(
//		fmt.Sprintf("http://%s:%d/key/%s", node, d.chronosDBPort, key.String()),
//		nil,
//		nil,
//		d.httpClient,
//		d.cfg.MaxRetries,
//		d.logger,
//		"dynamo.tellHasKey",
//	)
//
//	switch status {
//	case http.StatusOK:
//		return nil
//	case http.StatusNotFound:
//		return nil
//	default:
//		error := responsetypes.Error{}
//		err := json.Unmarshal(response, &error)
//		if err != nil {
//			return err
//		}
//
//		return errors.New(error.Message)
//	}
//}
