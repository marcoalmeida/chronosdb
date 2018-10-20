package shared

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// Min returns the smallest of two integers
func Min(a int, b int) int {
	if a < b {
		return a
	}

	return b
}

// exponential backoff
func Backoff(i int, caller string, logger *zap.Logger) {
	// 2^i -- this will always be used for very small values (number of retries), so the signed/unsigned type casts
	// are safe
	var wait int64 = 1
	if i > 0 {
		wait = 2 << (uint64(i) - 1)
	}
	// add jitter -- random(0, wait*100ms)
	wait = rand.Int63n(wait * 100)
	logger.Debug("Exponential back off", zap.Int64("ms", wait), zap.String("caller", caller))
	time.Sleep(time.Duration(wait) * time.Millisecond)
}

// initialize and return an HTTP client instance
func NewHTTPClient(connectTimeout int, clientTimeout int) *http.Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   time.Duration(connectTimeout) * time.Millisecond,
			DualStack: true,
		}).DialContext,
	}

	return &http.Client{
		Transport: tr,
		Timeout:   time.Duration(clientTimeout) * time.Millisecond,
	}
}

func sendHTTPRequest(
	url string,
	payload []byte,
	headers http.Header,
	method string,
	client *http.Client,
	maxRetries int,
	logger *zap.Logger,
	caller string,
) (int, []byte) {
	// we always want to return the status and body, so it must exist outside of the scope of the for loop
	var status int
	var err error
	var req *http.Request
	var body []byte

	for i := 0; i < maxRetries; i++ {
		var resp *http.Response

		req, err = http.NewRequest(method, url, bytes.NewReader(payload))
		if err != nil {
			logger.Error("Failed to create HTTP request", zap.Error(err))
		}

		for k, values := range headers {
			for _, v := range values {
				req.Header.Add(k, v)
			}
		}

		if method == "POST" {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}

		resp, err = client.Do(req)
		if err != nil {
			logger.Debug(
				"Failed "+method,
				zap.Int("attempt", i),
				zap.Error(err),
				zap.String("caller", caller),
			)
			Backoff(i, caller, logger)
			continue
		}

		body, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			logger.Debug("Failed to read the response body", zap.Error(err), zap.String("caller", caller))
			Backoff(i, caller, logger)
			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
			// success, we can stop here
			return resp.StatusCode, body
		} else {
			// client side error, no point on trying to continue
			if resp.StatusCode >= 400 && resp.StatusCode <= 499 {
				return resp.StatusCode, body
			}
			// server side error, could be a number of things; we should wait and retry
			if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
				// save for return
				status = resp.StatusCode
				Backoff(i, caller, logger)
			}
		}
	}

	// if we made it this far, the write failed
	// the status code will be 5XY or 0 (initialized as), depending on whether or not a connection was actually
	if err != nil {
		return status, []byte(err.Error())
	}

	return status, body
}

func DoGet(
	url string,
	headers http.Header,
	client *http.Client,
	maxRetries int,
	logger *zap.Logger,
	caller string,
) (int, []byte) {
	return sendHTTPRequest(url, nil, headers, "GET", client, maxRetries, logger, caller)
}

func DoPost(
	url string,
	payload []byte,
	headers http.Header,
	client *http.Client,
	maxRetries int,
	logger *zap.Logger,
	caller string,
) (int, []byte) {
	return sendHTTPRequest(url, payload, headers, "POST", client, maxRetries, logger, caller)
}

func DoPut(
	url string,
	payload []byte,
	headers http.Header,
	client *http.Client,
	maxRetries int,
	logger *zap.Logger,
	caller string,
) (int, []byte) {
	return sendHTTPRequest(url, payload, headers, "PUT", client, maxRetries, logger, caller)
}

func DoDelete(
	url string,
	payload []byte,
	headers http.Header,
	client *http.Client,
	maxRetries int,
	logger *zap.Logger,
	caller string,
) (int, []byte) {
	return sendHTTPRequest(url, payload, headers, "DELETE", client, maxRetries, logger, caller)
}
