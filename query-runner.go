package influxqb

import (
	"errors"
	influx "github.com/influxdata/influxdb1-client/v2"
	"time"
)

type queryRunner struct {
	client  *influx.Client
	queries []*queryBuilder
}

func NewQueryRunner(client *influx.Client) (*queryRunner, error) {
	qr := queryRunner{}
	qr.client = client
	return &qr, GetClientError(client)
}

func (qr *queryRunner) Add(q *queryBuilder) *queryRunner {
	qr.queries = append(qr.queries, q)
	return qr
}

func (qr *queryRunner) ExecuteQueries() (resp *response, err error) {
	var res *queryResult
	for i, query := range qr.queries {
		r, queryErr := qr.Do(query)
		err = queryErr

		if i == 0 {
			res = newQueryResult(r)
			continue
		}
		res.addData(r)
	}

	return res.export(), err
}

//Runs a single queryBuilder's query.
func (qr *queryRunner) Do(q *queryBuilder) (r []influx.Result, err error) {

	if err := GetClientError(qr.client); err != nil {
		return r, err
	}
	_, results, err := query(qr.client, q.database, q.String())
	if err == nil {
		if results[0].Err != "" {
			err = errors.New(results[0].Err)
			return results, err
		}
	}
	return results, err
}

func GetClientError(client *influx.Client) error {
	if client == nil {
		return errors.New("nil pointer to client")
	}
	if _, _, err := (*client).Ping(700 * time.Millisecond); err != nil {
		return err
	}
	return nil
}
