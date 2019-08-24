package inflx

import (
	influx "github.com/influxdata/influxdb1-client/v2"
)

func WritePoint(db string, tags map[string]string, fields map[string]interface{}) (err error) {

	bp, err := NewBatchPoints(db)

	if err != nil {
		return
	}

	point, err := NewStatisticPoint(tags, fields)

	if err != nil {
		return
	}

	bp.AddPoint(point)

	return WritePoints(bp)

}

func WritePoints(bp influx.BatchPoints) error {

	return Client.Write(bp)
}
