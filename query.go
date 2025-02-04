package influxqb

import (
	"github.com/influxdata/influxdb1-client/v2"
	inflx "github.com/influxdata/influxdb1-client/v2"
)

// queryDB convenience function to query the database
func query(cl *inflx.Client, db, cmd string) (response *client.Response, res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: db,
	}
	if response, err = (*cl).Query(q); err == nil {
		if response.Error() != nil {
			return response, res, response.Error()
		}
		res = response.Results
	}
	return response, res, err
}
