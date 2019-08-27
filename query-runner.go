package influxqb

import (
	"errors"
	influx "github.com/influxdata/influxdb1-client/v2"
)

type QueryRunner struct {
	queries []*HistogramBuilder
}

func NewQr() *QueryRunner {
	return &QueryRunner{}
}

var Client influx.Client

func NewClient(Host, Username, Password string) influx.Client {
	var err error
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     Host,
		Username: Username,
		Password: Password,
	})
	if err != nil {
		panic(errors.New("influx connection: " + err.Error()))
	}
	return c
}

func (qr *QueryRunner) Add(query *HistogramBuilder) *QueryRunner {
	qr.queries = append(qr.queries, query)
	return qr
}

func (qr *QueryRunner) ExecuteQueries() (res *Indexer, err error) {

	for i, query := range qr.queries {
		r, queryErr := Do(query)
		err = queryErr

		if i == 0 {
			res = NewHistogramData(r)
			continue
		}
		res.AddData(r)
	}

	return
}

func Do(h *HistogramBuilder) (r []influx.Result, err error) {

	_, results, err := Query(h.Database, h.Query())
	if err == nil {
		if results[0].Err != "" {
			err = errors.New(results[0].Err)
			return results, err
		}
	}
	return results, err
}
