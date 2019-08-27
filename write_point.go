package influxqb

import (
	"github.com/influxdata/influxdb1-client/v2"
	inflx "github.com/influxdata/influxdb1-client/v2"
)

func WritePoint(cl inflx.Client, TAGS map[string]string, fields map[string]interface{}) {
	batchPoints, err := client.NewBatchPoints(client.BatchPointsConfig{Database: "Sanjagh", Precision: "s"})
	point, err := NewStatisticPoint(TAGS, fields)
	if err != nil {
		panic(err)
	}
	batchPoints.AddPoint(point)
	err = cl.Write(batchPoints)
	if err != nil {
		panic(err)
	}
}
