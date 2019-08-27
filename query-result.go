package influxqb

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb1-client/models"
	"github.com/influxdata/influxdb1-client/v2"
	"github.com/mdaliyan/govert"
)

type queryResult struct {
	data     []client.Result
	dataSets map[string]string
	indexes  map[string]int
	config   map[string]map[string]interface{}
	summary  models.Row
}

func newQueryResult(data []client.Result) *queryResult {
	q := queryResult{}
	q.setData(data)
	return &q
}

func (qResult *queryResult) setData(data []client.Result) {
	qResult.data = data
	qResult.indexes = map[string]int{}
	qResult.config = map[string]map[string]interface{}{}
	qResult.summary = models.Row{
		Tags:    map[string]string{},
		Columns: []string{},
		Values:  [][]interface{}{},
	}
	for index, Row := range data {
		if Row.Series != nil {
			if len(Row.Series[0].Columns) == 2 && len(Row.Series[0].Values) != 1 {
				// it's time dataSets
				qResult.indexes[Row.Series[0].Columns[1]] = index
			} else {
				// it's summary
				qResult.summary = Row.Series[0]
			}
		}
	}
}

func (qResult *queryResult) addData(data []client.Result) {
	qResult.data = append(qResult.data, data...)
	for index, Row := range data {
		if Row.Series != nil {
			if len(Row.Series[0].Columns) == 2 && len(Row.Series[0].Values) != 1 {
				// it's time dataSets
				qResult.indexes[Row.Series[0].Columns[1]] = index //+offset
			} else {
				// it's summary
				if qResult.summary.Tags == nil {
					qResult.summary.Tags = map[string]string{}
				}
				for k, v := range Row.Series[0].Tags {
					qResult.summary.Tags[k] = v
				}
				qResult.summary.Columns = append(qResult.summary.Columns, Row.Series[0].Columns...)
				for j := range qResult.summary.Values {
					qResult.summary.Values[j] = append(qResult.summary.Values[j], Row.Series[0].Values[j]...)
				}
			}
		}
	}

}

func (qResult *queryResult) GetTimeSeriesFor(key string) [][]interface{} {
	index, ok := qResult.indexes[key]
	if !ok {
		return [][]interface{}{}
	}
	return qResult.data[index].Series[0].Values
}

func (qResult *queryResult) GetSummary() (series map[string]interface{}) {
	series = map[string]interface{}{}
	for index, name := range qResult.summary.Columns {
		if name != "time" {
			series[name] = govert.Float64(qResult.summary.Values[0][index])
		}
	}
	return
}

func (qResult *queryResult) GetSummaryValue(key string) json.Number {
	for index, name := range qResult.summary.Columns {
		if name == key {
			val := qResult.summary.Values[0][index]
			if val != nil {
				return val.(json.Number)
			} else {
				return json.Number("0")
			}
		}
	}
	return json.Number("0")
}

func (qResult *queryResult) export() (r *response) {
	r = &response{
		Summary:  qResult.GetSummary(),
		DataSets: dataSets{},
	}
	for key := range qResult.dataSets {
		r.DataSets[key] = qResult.GetTimeSeriesFor(key)
	}
	return
}

func (qResult *queryResult) String() (str string) {

	if qResult == nil {
		str = "[empty queryResult]"
		return
	}
	str = ""
	str += fmt.Sprintln("\nSeries:")
	for _, result := range qResult.data {
		str += fmt.Sprintln(result.Series[0])
	}
	str += fmt.Sprintln("\nSummary:")
	j := 0
	for k, v := range qResult.GetSummary() {
		str += fmt.Sprintln(j, ". ", k, " : ", v)
		j++
	}
	return
}
