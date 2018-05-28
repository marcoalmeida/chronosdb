package dynamo

import (
	"fmt"
	"net/url"

	"github.com/marcoalmeida/chronosdb/coretypes"
)

// return true iff the current node is coordinating this request, i.e., the `forward` parameter has not been set
func (dyn *Dynamo) nodeIsCoordinator(form url.Values) bool {
	return form.Get("forward") == ""
}

// generate a URL to be used for forwarding a request
func (dyn *Dynamo) createForwardURL(node string, uri string) string {
	u, err := url.Parse(fmt.Sprintf("http://%s:%d%s", node, dyn.chronosDBPort, uri))
	if err != nil {
		return ""
	}
	q := u.Query()
	q.Set("forward", "false")
	u.RawQuery = q.Encode()

	return u.String()
}

func (dyn *Dynamo) setHintedHandoffURL(uri string) string {
	u, err := url.ParseRequestURI(uri)
	if err != nil {
		return ""
	}

	q := u.Query()
	q.Set("hint", "true")
	u.RawQuery = q.Encode()

	return u.String()
}

// return true iff the current request is a hint being handed off
func (dyn *Dynamo) requestIsHintedHandoff(form url.Values) bool {
	return form.Get("hint") == "true"

}

// not pretty, tightly coupled with an InfluxDB URL and overloading it, but we need to create the URL somehow and
// somewhere.
// one day we may have our own interface and a layer for compatibility with InfluxDB and this will be the right place
// to create this type of URL
func (dyn *Dynamo) createKeyTransferURL(key *coretypes.Key) string {
	return fmt.Sprintf("/write?db=%s&key=%s&transfer=true", key.DB, key.String())
}

// return true iff the current request is part of a key transfer
func (dyn *Dynamo) requestIsKeyTransfer(form url.Values) bool {
	return form.Get("transfer") == "true"
}

func (dyn *Dynamo) getKeyFromURL(form url.Values) *coretypes.Key {
	return coretypes.KeyFromString(form.Get("key"))
}
