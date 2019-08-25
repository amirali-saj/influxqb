package qb

import (
	"encoding/json"

	"github.com/influxdata/influxdb1-client/models"
	"github.com/influxdata/influxdb1-client/v2"
	"github.com/mdaliyan/govert"
)

type Indexer struct {
	data    []client.Result
	indexes map[string]int
	config  map[string]map[string]interface{}
	summary models.Row
}

func NewHistogramData(data []client.Result) *Indexer {
	i := Indexer{}
	i.setData(data)
	return &i
}

func (i *Indexer) setData(data []client.Result) {
	i.data = data
	i.indexes = map[string]int{}
	i.config = map[string]map[string]interface{}{}
	i.summary = models.Row{
		Tags:    map[string]string{},
		Columns: []string{},
		Values:  [][]interface{}{},
	}
	for index, Row := range data {
		if Row.Series != nil {
			if len(Row.Series[0].Columns) == 2 && len(Row.Series[0].Values) != 1 {
				// it's time dataSets
				i.indexes[Row.Series[0].Columns[1]] = index
			} else {
				// it's summary
				i.summary = Row.Series[0]
			}
		}
	}
}

func (i *Indexer) GetTimeSeriesFor(key string) [][]interface{} {
	index, ok := i.indexes[key]
	if !ok {
		return [][]interface{}{}
	}
	return i.data[index].Series[0].Values
}

func (i *Indexer) GetSummary() (series map[string]interface{}) {
	series = map[string]interface{}{}
	for index, name := range i.summary.Columns {
		if name != "time" {
			series[name] = govert.Float64(i.summary.Values[0][index])
		}
	}
	return
}

func (i *Indexer) GetSummaryValue(key string) json.Number {
	for index, name := range i.summary.Columns {
		if name == key {
			val := i.summary.Values[0][index]
			if val != nil {
				// fmt.Println(reflect.TypeOf(val).String() == "json.Number")
				return val.(json.Number)
			} else {
				return json.Number("0")
			}
		}
	}
	return json.Number("0")
}

// func (i *Indexer) Index(s string) (index int) {
// 	i.l.Lock()
// 	index, ok := i.m[s]
// 	if ! ok {
// 		index = -1
// 	}
// 	defer i.l.Unlock()
// 	return
// }
