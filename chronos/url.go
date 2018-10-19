package chronos

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/marcoalmeida/chronosdb/coretypes"
)

// query string parameters used by ChronosDB (appended to whatever InfluxDB uses for internal use)
const (
	headerXForward = "X-ChronosDB-Forward"
	headerXKey     = "X-ChronosDB-Key"
	qsHandoff      = "xdbHandoff"
	qsTransfer     = "xdbTransfer"
	qsKey          = "xdbKey"
)

// return true iff the current node is coordinating this request, i.e., the `forward` header has not been set
func (c *Chronos) nodeIsCoordinator(headers http.Header) bool {
	return headers.Get(headerXForward) == ""
}

// generate a URL to be used for forwarding a request
func (c *Chronos) createForwardURL(node string, uri string) string {
	return fmt.Sprintf("http://%s:%c%s", node, c.cfg.Port, uri)
}

func createForwardHeaders(key *coretypes.Key) http.Header {
	headers := http.Header{}
	headers.Set(headerXForward, "true")
	headers.Set(headerXKey, key.String())

	return headers
}

// expand and existing URL to include the internal query string marker that signals a handoff
// also include the key being handed off just for convenience
func (c *Chronos) createHandoffURL(uri string, key *coretypes.Key) string {
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
func (c *Chronos) requestIsHintedHandoff(form url.Values) bool {
	return form.Get(qsHandoff) == "true"

}

// TODO
// not pretty, tightly coupled with an InfluxDB URL and overloading it, but we need to create the URL somehow,
// somewhere.
// maybe the influxdb package could return a base URL that this function then expands (similarly to createHandoffURL)?
func (c *Chronos) createKeyTransferURL(key *coretypes.Key) string {
	return fmt.Sprintf("/write?db=%s&%s=%s&%s=true", key.DB, qsKey, key.String(), qsTransfer)
}

// return true iff the current request is part of a key transfer
func (c *Chronos) requestIsKeyTransfer(form url.Values) bool {
	return form.Get(qsTransfer) == "true"
}

func getKeyFromURL(form url.Values) *coretypes.Key {
	return coretypes.KeyFromString(form.Get("key"))
}

func getKeyFromRequest(headers http.Header) *coretypes.Key {
	return coretypes.KeyFromString(headers.Get(headerXKey))
}
