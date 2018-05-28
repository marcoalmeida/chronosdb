package influxdb

import (
	"bytes"
	"io/ioutil"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/deckarep/golang-set"
)

const samplePayload = "Action,action=user/create,cluster=ec2,environment=development count=1i 1492560394223581000\n" +
	"Apache,action=user/create,cluster=ec2,environment=development count=1i 1492569215476420000\n" +
	"Action,action=user/create,cluster=ec2,environment=development count=1i 1492641065527185000\n" +
	"Apache,action=user/create,cluster=ec2,environment=development count=1i 1492641063653314000\n" +
	"nginx,action=user/create,cluster=ec2,environment=development count=1i 1492641067084446000\n" +
	"Action,action=user/create,cluster=ec2,environment=development count=1i 1492642735366440000\n" +
	"nginx,action=user/create,cluster=ec2,environment=development count=1i 1492560748393055000"

var expectedSplitMeasurements map[string][]byte = map[string][]byte{
	"Action": []byte("Action,action=user/create,cluster=ec2,environment=development count=1i 1492560394223581000\n" +
		"Action,action=user/create,cluster=ec2,environment=development count=1i 1492641065527185000\n" +
		"Action,action=user/create,cluster=ec2,environment=development count=1i 1492642735366440000\n"),
	"Apache": []byte("Apache,action=user/create,cluster=ec2,environment=development count=1i 1492569215476420000\n" +
		"Apache,action=user/create,cluster=ec2,environment=development count=1i 1492641063653314000\n"),
	"nginx": []byte("nginx,action=user/create,cluster=ec2,environment=development count=1i 1492641067084446000\n" +
		"nginx,action=user/create,cluster=ec2,environment=development count=1i 1492560748393055000\n"),
}

func TestSplitMeasurements(t *testing.T) {
	empty := SplitMeasurements([]byte(""))
	if len(empty) > 0 {
		t.Error("Expected empty list, got ", empty)
	}

	measurements := SplitMeasurements([]byte(samplePayload))
	if !reflect.DeepEqual(measurements, expectedSplitMeasurements) {
		t.Error("Split measurements mismatch")
	}
}

func TestExtractQueries(t *testing.T) {
	var queries []string

	q0 := "SELECT \"value\" FROM \"cpu_load_short\" WHERE \"region\"='us-west'"
	q1 := "SELECT nope"

	// nothing
	v := url.Values{}
	queries = QueriesFromURL(v)
	if !reflect.DeepEqual(queries, []string{}) {
		t.Error("Expected empty list, got", queries, len(queries))
	}

	// empty string
	v.Set("q", "")
	queries = QueriesFromURL(v)
	if !reflect.DeepEqual(queries, []string{}) {
		t.Error("Expected empty list, got", queries, len(queries))
	}

	// single query, with and without terminating semi-solon
	for _, q := range []string{q0, q0 + ";", q0 + " ; "} {
		v.Set("q", q)
		queries = QueriesFromURL(v)
		if strings.TrimSpace(queries[0]) != q0 {
			t.Error("Expected", q0, " got", queries[0])
		}
	}

	// two queries
	for _, q := range []string{q0 + ";" + q1, q0 + " ; " + q1} {
		v.Set("q", q)
		queries = QueriesFromURL(v)
		if strings.TrimSpace(queries[0]) != q0 || strings.TrimSpace(queries[1]) != q1 {
			t.Error("Expected", q0, "and", q1, ", got", queries[0])
		}
	}
}

func TestExtractMeasurement(t *testing.T) {
	// empty string, broken SELECT, regular expressions
	// ChronosDB cannot handle measurements defined as regular expressions, might as well keep the parsing side of
	// things simple and just fail on such cases
	brokenQueries := []string{"", "SELECT", "SELECT MEAN(\"degrees\") FROM /temperature/"}
	// multiple forms of SELECT statements
	simpleQueries := map[string][]string{
		"\"h2o_feet\"": {
			"SELECT \"level description\",\"location\",\"water_level\" FROM \"h2o_feet\"",
			"SELECT \"level description\"::field,\"location\"::tag,\"water_level\"::field FROM \"h2o_feet\"",
			"SELECT *::field FROM \"h2o_feet\"",
			"SELECT (\"water_level\" * 2) + 4 from \"h2o_feet\"",
			"SELECT /l/ FROM \"h2o_feet\" LIMIT 1",
			"SELECT MEAN(water_level) FROM \"h2o_feet\" WHERE \"location\" =~ /[m]/ AND \"water_level\" > 3",
		},
		"\"NOAA_water_database\".\"autogen\".\"h2o_feet\"": {
			"SELECT * FROM \"NOAA_water_database\".\"autogen\".\"h2o_feet\"",
		},
		"\"NOAA_water_database\"..\"h2o_feet\"": {"SELECT * FROM \"NOAA_water_database\"..\"h2o_feet\""},
		// multiple measurements
		"\"h2o_feet\",\"h2o_pH\"": {"SELECT * FROM \"h2o_feet\",\"h2o_pH\""},
	}

	for _, q := range brokenQueries {
		m := MeasurementNameFromQuery(q)
		if m != "" {
			t.Error("Expected empty string, got", m)
		}
	}

	for measurement, queries := range simpleQueries {
		for _, query := range queries {
			for _, q := range []string{query, query + " WHERE", query + " WHERE foo"} {
				got := MeasurementNameFromQuery(q)
				if measurement != got {
					t.Error("Expected", measurement, "got", got, "when extracting", q)
				}
			}
		}
	}
}

func Test_csvToLineProtocol(t *testing.T) {
	raw, err := ioutil.ReadFile("http_response_test.csv")
	if err != nil {
		t.Error("Failed to load CSV:", err)
	}

	// the line protocol requires at least 1 field
	fields := mapset.NewSet()
	fields.Add("BusyWorkers")

	lp, err := resultToLineProtocol(raw, fields)
	if err != nil {
		t.Error("Failed to translate to line protocol:", err)
	}
	checkLineProtocolFields(lp, t)

	// now with 2 fields
	fields.Add("BytesPerReq")
	lp, err = resultToLineProtocol(raw, fields)
	if err != nil {
		t.Error("Failed to translate to line protocol:", err)
	}
	checkLineProtocolFields(lp, t)
}

// check for a couple of random fields on the first line
func checkLineProtocolFields(lp []byte, t *testing.T) {
	matches := 0
	lines := bytes.Split(lp, []byte("\n"))
	for _, field := range bytes.Split(lines[0], []byte(",")) {
		if bytes.Equal(field, []byte("CPUChildrenSystem=0")) {
			matches++
		}
		if bytes.Equal(field, []byte("BytesPerSec=105.533")) {
			matches++
		}
	}
	if matches != 2 {
		t.Error("Matched", matches, "fields, expected to match 2")
	}

}
