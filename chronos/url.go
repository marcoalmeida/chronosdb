package chronos

import (
	"fmt"
	"net/url"

	"github.com/marcoalmeida/chronosdb/coretypes"
)

// query string parameters used by ChronosDB (appended to whatever InfluxDB uses for internal use)
const (
	qsForward  = "xdbForward"
	qsHandoff  = "xdbHandoff"
	qsTransfer = "xdbTransfer"
	qsKey      = "xdbKey"
)

// return true iff the current node is coordinating this request, i.e., the `forward` parameter has not been set
func (d *Chronos) nodeIsCoordinator(form url.Values) bool {
	return form.Get(qsForward) == ""
}

// generate a URL to be used for forwarding a request
func (d *Chronos) createForwardURL(node string, uri string) string {
	u, err := url.Parse(fmt.Sprintf("http://%s:%d%s", node, d.cfg.Port, uri))
	if err != nil {
		return ""
	}
	q := u.Query()
	q.Set(qsForward, "false")
	u.RawQuery = q.Encode()

	return u.String()
}

// expand and existing URL to include the internal query string marker that signals a handoff
// also include the key being handed off just for convenience
func (d *Chronos) createHandoffURL(uri string, key *coretypes.Key) string {
	u, err := url.ParseRequestURI(uri)
	if err != nil {
		return ""
	}

	q := u.Query()
	q.Set(qsHandoff, "true")
	q.Set(qsKey, key.String())
	u.RawQuery = q.Encode()

	return u.String()
}

// return true iff the current request is a hint being handed off
func (d *Chronos) requestIsHintedHandoff(form url.Values) bool {
	return form.Get(qsHandoff) == "true"

}

// TODO
// not pretty, tightly coupled with an InfluxDB URL and overloading it, but we need to create the URL somehow,
// somewhere.
// maybe the influxdb package could return a base URL that this function then expands (similarly to createHandoffURL)?
func (d *Chronos) createKeyTransferURL(key *coretypes.Key) string {
	return fmt.Sprintf("/write?db=%s&%s=%s&%s=true", key.DB, qsKey, key.String(), qsTransfer)
}

// return true iff the current request is part of a key transfer
func (d *Chronos) requestIsKeyTransfer(form url.Values) bool {
	return form.Get(qsTransfer) == "true"
}

func (d *Chronos) getKeyFromURL(form url.Values) *coretypes.Key {
	return coretypes.KeyFromString(form.Get("key"))
}
