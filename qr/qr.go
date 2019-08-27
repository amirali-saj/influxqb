package qr

import (
	"errors"
	"fmt"
	cl "github.com/influxdata/influxdb1-client/v2"
	"github.com/mdaliyan/influxqb/inflx"
	"github.com/mdaliyan/influxqb/qb"
)

func ExecuteQueries(queries ...*qb.HistogramBuilder) *qb.Indexer {

	var res *qb.Indexer

	for i, query := range queries {
		r, _ := Do(query)
		fmt.Println("\n"+query.Database+"."+query.Measurement, "{\n", r, "\n}")

		if i == 0 {
			res = qb.NewHistogramData(r)
			continue
		}
		res.AddData(r)
	}
	return res
}

func Do(h *qb.HistogramBuilder) (r []cl.Result, err error) {
	_, results, err := inflx.Query(h.Database, h.Query())
	if err == nil {
		if results[0].Err != "" {
			err = errors.New(results[0].Err)
			return results, err
		}
	}
	return results, err
}
