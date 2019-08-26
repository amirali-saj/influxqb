package inflx

import (
	"github.com/influxdata/influxdb1-client/v2"
)

func WritePoint(TAGS map[string]string, fields map[string]interface{}) {
	batchPoints, err := client.NewBatchPoints(client.BatchPointsConfig{Database: "Sanjagh", Precision: "s"})
	point, err := NewStatisticPoint(TAGS, fields)
	if err != nil {
		panic(err)
	}
	batchPoints.AddPoint(point)
	err = Client.Write(batchPoints)
	if err != nil {
		panic(err)
	}
}
