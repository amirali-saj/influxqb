package influxqb

import (
	"time"

	influx "github.com/influxdata/influxdb1-client/v2"
)

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
