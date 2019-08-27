package influxqb

import (
	"errors"
	influx "github.com/influxdata/influxdb1-client/v2"
)

type QueryRunner struct {
	client  *influx.Client
	queries []*QueryBuilder
}

func NewQueryRunner(client *influx.Client) *QueryRunner {
	qr := QueryRunner{}
	qr.client = client
	return &qr
}

func (qr *QueryRunner) Add(q *QueryBuilder) *QueryRunner {
	qr.queries = append(qr.queries, q)
	return qr
}

func (qr *QueryRunner) ExecuteQueries() (res *Indexer, err error) {

	for i, query := range qr.queries {
		r, queryErr := qr.Do(query)
		err = queryErr

		if i == 0 {
			res = newHistogramData(r)
			continue
		}
		res.addData(r)
	}

	return
}

//Runs a single QueryBuilder's query.
func (qr *QueryRunner) Do(q *QueryBuilder) (r []influx.Result, err error) {

	_, results, err := query(qr.client, q.database, q.Query())
	if err == nil {
		if results[0].Err != "" {
			err = errors.New(results[0].Err)
			return results, err
		}
	}
	return results, err
}
