package influxdb

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"

	"github.com/deckarep/golang-set"
	"github.com/marcoalmeida/chronosdb/shared"
	"go.uber.org/zap"
)

const (
	DefaultPort           = 8086
	DefaultConnectTimeout = 500
	DefaultClientTimeout  = 3000
	DefaultMaxRetries     = 3
)

type userInfo struct {
	User     string
	Password string
}

type Cfg struct {
	Port           int64
	ConnectTimeout int    `toml:"connect_timeout"`
	ClientTimeout  int    `toml:"client_timeout"`
	MaxRetries     int    `toml:"max_retries"`
	AdminUser      string `toml:"admin_user"`
	AdminPassword  string `toml:"admin_password"`
	Credentials    map[string]userInfo
}

type InfluxDB struct {
	cfg                  *Cfg `toml:"influxdb"`
	logger               *zap.Logger
	httpClient           *http.Client
	HTTPStatusSuccessful int
}

// TODO: switch to msgpack? maybe? should we grab these structs from official InfluxDB?
// this json format seems very convoluted, fragile, error-prone, ...

type series struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Values  [][]interface{} `json:"values"`
}

type result struct {
	StatementID int64    `json:"statement_id"`
	Series      []series `json:"series"`
}

type httpResponse struct {
	Results []result `json:"results"`
}

type httpError struct {
	Error string
}

// GenerateWriteURI returns an InfluxDB write endpoint for the given DB
func GenerateWriteURI(db string) string {
	if db == "" {
		return ""
	}

	return fmt.Sprintf("/write?db=%s", db)
}

func New(cfg *Cfg, logger *zap.Logger) *InfluxDB {
	return &InfluxDB{
		cfg:                  cfg,
		logger:               logger,
		httpClient:           shared.NewHTTPClient(cfg.ConnectTimeout, cfg.ClientTimeout),
		HTTPStatusSuccessful: http.StatusNoContent,
	}
}

func (idb *InfluxDB) IsAlive() bool {
	u := idb.createURL("/ping", "", false)
	status, _ := shared.DoGet(u, nil, idb.httpClient, idb.cfg.MaxRetries, idb.logger)

	return status >= 200 && status <= 299
}

func (idb *InfluxDB) createURL(uri string, db string, adminCredentials bool) string {
	var user, password string

	if adminCredentials {
		user = idb.cfg.AdminUser
		password = idb.cfg.AdminPassword
	} else {
		if creds, ok := idb.cfg.Credentials[db]; ok {
			user = creds.User
			password = creds.Password
		}
	}

	if user != "" && password != "" {
		return fmt.Sprintf("http://%s:%s@localhost:%d%s", user, password, idb.cfg.Port, uri)
	} else {
		return fmt.Sprintf("http://localhost:%d%s", idb.cfg.Port, uri)
	}
}

// any error message we return should be a simple string, not json; InfluxDB *sometimes* returns json
func (idb *InfluxDB) handleHTTPError(response []byte) error {
	// the error message should be a simple string, not json; InfluxDB *sometimes* returns json
	e := httpError{}
	err := json.Unmarshal(response, &e)

	if err != nil {
		idb.logger.Error("Failed to unmarshal HTTP response", zap.Error(err), zap.ByteString("response", response))
		return errors.New(string(response))
	}

	return errors.New(e.Error)
}

func (idb *InfluxDB) ShowDBs() ([]string, error) {
	dbs := []string{}
	url := idb.createURL("/query", "", true)
	payload := "q=SHOW DATABASES"
	status, response := shared.DoPost(
		url,
		[]byte(payload),
		nil,
		idb.httpClient,
		idb.cfg.MaxRetries,
		idb.logger,
	)
	if !(status >= 200 && status <= 299) {
		return nil, idb.handleHTTPError(response)
	}

	r := httpResponse{}
	err := json.Unmarshal(response, &r)
	if err != nil {
		return nil, err
	}

	// no data; weird, but not an error
	if len(r.Results) != 1 {
		return dbs, nil
	}

	if len(r.Results[0].Series) == 0 {
		return dbs, nil
	}

	for _, v := range r.Results[0].Series[0].Values {
		// we should have a list with exactly 1 element
		if len(v) != 1 {
			idb.logger.Error("Expected a singleton with a DB name")
		} else {
			// skip the internal DB
			if v[0] != "_internal" {
				dbs = append(dbs, v[0].(string))
			}
		}
	}

	return dbs, nil
}

func (idb *InfluxDB) ShowMeasurements(db string) ([]string, error) {
	measurements := []string{}
	u := idb.createURL("/query", "", true)
	payload := fmt.Sprintf("q=SHOW MEASUREMENTS ON %s", db)
	status, response := shared.DoPost(
		u,
		[]byte(payload),
		nil,
		idb.httpClient,
		idb.cfg.MaxRetries,
		idb.logger,
	)
	if !(status >= 200 && status <= 299) {
		return nil, idb.handleHTTPError(response)
	}

	r := httpResponse{}
	err := json.Unmarshal(response, &r)
	if err != nil {
		return nil, err
	}

	// no data; weird, but not an error
	if len(r.Results) != 1 {
		return measurements, nil
	}

	if len(r.Results[0].Series) == 0 {
		return measurements, nil
	}

	for _, v := range r.Results[0].Series[0].Values {
		// we should have a list with exactly 1 element
		if len(v) != 1 {
			idb.logger.Error("Expected a singleton with a measurement name")
		} else {
			measurements = append(measurements, v[0].(string))
		}
	}

	return measurements, nil
}

func (idb *InfluxDB) ShowFieldKeys(db string, measurement string) (mapset.Set, error) {
	fields := mapset.NewSet()
	u := idb.createURL("/query", "", true)
	payload := fmt.Sprintf("q=SHOW FIELD KEYS ON %s FROM %s", db, measurement)
	status, response := shared.DoPost(
		u,
		[]byte(payload),
		nil,
		idb.httpClient,
		idb.cfg.MaxRetries,
		idb.logger,
	)
	if !(status >= 200 && status <= 299) {
		return nil, idb.handleHTTPError(response)
	}

	r := httpResponse{}
	err := json.Unmarshal(response, &r)
	if err != nil {
		return nil, err
	}

	// no data; weird, but not an error
	if len(r.Results) != 1 {
		return fields, nil
	}

	if len(r.Results[0].Series) == 0 {
		return fields, nil
	}

	for _, v := range r.Results[0].Series[0].Values {
		// we should have a list with 2 elements: field key, and data type
		if len(v) != 2 {
			idb.logger.Error("Expected a singleton with a measurement name")
		} else {
			fields.Add(v[0].(string))
		}
	}

	return fields, nil
}

func (idb *InfluxDB) createOrDropDB(db string, action string) error {
	u := idb.createURL("/query", db, true)
	payload := fmt.Sprintf("q=%s DATABASE %s", action, db)

	status, response := shared.DoPost(
		u,
		[]byte(payload),
		nil,
		idb.httpClient,
		idb.cfg.MaxRetries,
		idb.logger,
	)

	if !(status >= 200 && status <= 299) {
		// return idb.handleHTTPError(response)
		idb.logger.Error(
			"InfluxDB failed to create/drop a database",
			zap.String("db", db),
			zap.String("action", action),
			zap.ByteString("response", response),
		)

		return errors.New("backend failed; check the logs")
	}

	return nil
}

func (idb *InfluxDB) CreateDB(db string) error {
	idb.logger.Info("Creating DB", zap.String("db", db))
	return idb.createOrDropDB(db, "CREATE")
}

func (idb *InfluxDB) DropDB(db string) error {
	idb.logger.Info("Dropping DB", zap.String("db", db))
	return idb.createOrDropDB(db, "DROP")
}

func (idb *InfluxDB) Write(uri string, db string, metrics []byte) (int, []byte) {
	u := idb.createURL(uri, db, false)
	return shared.DoPost(
		u,
		metrics,
		nil,
		idb.httpClient,
		idb.cfg.MaxRetries,
		idb.logger,
	)
}

func (idb *InfluxDB) Query(uri string, form url.Values) (int, []byte) {
	db := DBNameFromURL(form)
	if db == "" {
		// DB not passed??
		return http.StatusBadRequest, []byte("DB parameter not found")
	}

	u := idb.createURL(uri, db, false)
	return shared.DoPost(
		u,
		[]byte(form.Encode()),
		nil,
		idb.httpClient,
		idb.cfg.MaxRetries,
		idb.logger,
	)
}

//// merge an array of responses into a single JSON object
//func (idb *InfluxDB) MergeResponses(responses [][]byte, pretty bool) ([]byte, error) {
//	merged := httpResponse{}
//
//	for _, response := range responses {
//		r := httpResponse{}
//		err := json.Unmarshal(response, &r)
//		if err != nil {
//			idb.logger.Debug("Failed to unmarshal response", zap.ByteString("response", response))
//			return nil, err
//		}
//
//		if len(r.Results) >= 1 {
//			merged.Results = append(merged.Results, r.Results...)
//		} else {
//			idb.logger.Debug("Got empty response", zap.ByteString("response", response))
//		}
//	}
//
//	var result []byte
//	var err error
//	if pretty {
//		result, err = json.MarshalIndent(merged, "", "  ")
//	} else {
//		result, err = json.Marshal(merged)
//	}
//
//	if err != nil {
//		return nil, err
//	}
//
//	return result, nil
//}

// TODO: replace this with a call to idb.Query that sets the Accept header to application/csv; then the caller
// TODO: can compose the result with resultToLineProtocol
// query InfluxDB to get all the data from a fiven measurement and return the results in the form of line protocol
func (idb *InfluxDB) QueryToLineProtocol(db string, measurement string, limit int, offset int) ([]byte, error) {
	uri := fmt.Sprintf("/query?db=%s", db)
	url := idb.createURL(uri, db, false)
	q := fmt.Sprintf("q=SELECT * FROM %s LIMIT %d OFFSET %d", measurement, limit, offset)

	headers := http.Header{}
	headers.Set("Accept", "application/csv")

	status, response := shared.DoPost(
		url,
		[]byte(q),
		headers,
		idb.httpClient,
		idb.cfg.MaxRetries,
		idb.logger,
	)

	if !(status >= 200 && status <= 299) {
		return nil, errors.New(fmt.Sprintf("status: %d, respnse %v", status, response))
	}

	fields, err := idb.ShowFieldKeys(db, measurement)
	if err != nil {
		return nil, err
	}

	return resultToLineProtocol(response, fields)
}

// https://docs.influxdata.com/influxdb/v1.5/write_protocols/line_protocol_reference/
func resultToLineProtocol(csvData []byte, fields mapset.Set) ([]byte, error) {
	tagIndexes := mapset.NewSet()
	fieldIndexes := mapset.NewSet()
	timeIndex := 0
	lineProtocol := bytes.Buffer{}

	r := csv.NewReader(bytes.NewReader(csvData))
	// read 1 record to process the header
	header, err := r.Read()
	if err == io.EOF {
		return lineProtocol.Bytes(), nil
	}
	if err != nil {
		return nil, err
	}
	// get the index of each column and map it to either a tag or a field
	for i, csvField := range header {
		// there's always a column for time
		if csvField == "time" {
			timeIndex = i
		} else {
			if fields.Contains(csvField) {
				fieldIndexes.Add(i)
			} else {
				tagIndexes.Add(i)
			}
		}
	}
	// process all records
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// measurement
		lineProtocol.WriteString(record[0])
		// accumulate all tags
		itT := tagIndexes.Iterator()
		for t := range itT.C {
			if record[t.(int)] != "" {
				lineProtocol.Write([]byte(","))
				lineProtocol.WriteString(header[t.(int)])
				lineProtocol.Write([]byte("="))
				lineProtocol.WriteString(record[t.(int)])
			}
		}
		lineProtocol.Write([]byte(" "))
		// accumulate all fields
		itF := fieldIndexes.Iterator()
		i := 0
		for t := range itF.C {
			if record[t.(int)] != "" {
				// only print the delimiter (comma) if this is not the first field
				if i != 0 {
					lineProtocol.Write([]byte(","))
				}
				lineProtocol.WriteString(header[t.(int)])
				lineProtocol.Write([]byte("="))
				lineProtocol.WriteString(record[t.(int)])
				i++
			}
		}
		// timestamp
		lineProtocol.Write([]byte(" "))
		lineProtocol.WriteString(record[timeIndex])
		lineProtocol.Write([]byte("\n"))
	}

	return lineProtocol.Bytes(), nil
}

func SplitMeasurements(payload []byte) map[string][]byte {
	// TODO: optimize this!
	// format:
	// <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]

	if bytes.Equal(payload, []byte("")) {
		return nil
	}

	measurements := make(map[string][]byte)

	for _, line := range bytes.Split(payload, []byte("\n")) {
		if !bytes.Equal(line, []byte("")) {
			m := string(bytes.Split(line, []byte(","))[0])
			measurements[m] = append(measurements[m], line...)
			measurements[m] = append(measurements[m], byte('\n'))
		}
	}

	return measurements
}

// pretty basic; we keep it here mainly for abstraction
func DBNameFromURL(form url.Values) string {
	return form.Get("db")
}

//func QueriesFromURL(form url.Values) []string {
//	q := form.Get("q")
//	if q == "" {
//		return []string{}
//	}
//
//	return strings.Split(q, ";")
//}

// QueryFromURL returns the `SELECT` statement from the `q` parameter. As of now, ChronosDB expects only one
// `SELECT` statement per call to `/query`.
func QueryFromURL(form url.Values) string {
	return form.Get("q")
}

// the measurement(s) must be double quoted; there will always be whitespace before the opening quote and after the
// closing one; will fail if the name of the measurement is defined using a regular expression
var reMatchFROM = regexp.MustCompile(`(?i)^.*\s+FROM\s+(".*?")(\s+.*)?$`)

// TODO: use parsing functions from InfluxDB
func MeasurementNameFromQuery(query string) string {
	// InfluxDB imposes no restrictions on measurement's names which makes parsing queries much more complicated
	// trying to keep things simple, especially for the MVP, we require measurements to be double quoted and
	// implement a simple FSM to match them
	//
	// TODO: consider a different approach to parsing queries (grammar?)

	matches := reMatchFROM.FindStringSubmatch(query)

	if len(matches) == 0 {
		return ""
	}

	return matches[1]
}
