package influxqb

import (
	"errors"
	influx "github.com/influxdata/influxdb1-client/v2"
)

type QueryRunner struct {
	Client  influx.Client
	queries []*HistogramBuilder
}

func NewQueryRunner(client influx.Client) *QueryRunner {
	qr := QueryRunner{}
	qr.Client = client
	return &qr
}

func (qr *QueryRunner) Add(query *HistogramBuilder) *QueryRunner {
	qr.queries = append(qr.queries, query)
	return qr
}

func (qr *QueryRunner) ExecuteQueries() (res *Indexer, err error) {

	for i, query := range qr.queries {
		r, queryErr := qr.Do(query)
		err = queryErr

		if i == 0 {
			res = NewHistogramData(r)
			continue
		}
		res.AddData(r)
	}

	return
}

func (qr *QueryRunner) Do(h *HistogramBuilder) (r []influx.Result, err error) {

	_, results, err := Query(qr.Client, h.Database, h.Query())
	if err == nil {
		if results[0].Err != "" {
			err = errors.New(results[0].Err)
			return results, err
		}
	}
	return results, err
}
