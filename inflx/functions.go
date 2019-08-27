package inflx

import (
	"errors"
	"time"

	influx "github.com/influxdata/influxdb1-client/v2"
)

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

func NewBatchPoints(db string) (influx.BatchPoints, error) {
	bp, err := influx.NewBatchPoints(influx.BatchPointsConfig{Database: db})
	if err != nil {
		panic(err)
	}
	return bp, err
}

func NewStatisticPoint(TAGS map[string]string, fields map[string]interface{}) (*influx.Point, error) {
	t := time.Now()
	fields["timestamp"] = t.UnixNano()
	return influx.NewPoint("statistics", TAGS, fields, t)
}
