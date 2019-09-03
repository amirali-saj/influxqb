package main

import (
	"fmt"
	"github.com/influxdata/influxdb1-client/models"
	influx "github.com/influxdata/influxdb1-client/v2"
	inflx "github.com/mdaliyan/influxqb"
	"time"
)

func main() {

	clientMock := inflx.NewInfluxClientMock()

	fmt.Println(clientMock.Ping(25 * time.Second))

	influxClient := influx.Client(&clientMock)

	//Passing it to QueryRunner just like real influx client.
	qr, err := inflx.NewQueryRunner(&influxClient)

	if err != nil {
		panic(err)
	}

	//Creating mock database and mock measurements in Influx-db mock.
	// To use measurements with other rps just use rp_name.measurement_name as measurement name.

	clientMock.CreateDb("ad_network")
	clientMock.CreateMeasurement("ad_network", "clicks")
	clientMock.CreateMeasurement("ad_network", "views")

	//You can add a row into a specific measurement this way.
	clientMock.AddRowToMeasurement("ad_network", "clicks", models.Row{Tags: map[string]string{"ad_id": "ad2"}, Columns: []string{"time", "clicks"}, Values: [][]interface{}{{"0", 0}, {"5", 25}, {"10", 48}, {"15", 130}, {"45", 451}}})

	//Or you can create a batchPoint of points and write it to your mock influx database using clientMock

	bp, _ := influx.NewBatchPoints(influx.BatchPointsConfig{Database: "ad_network", Precision: "s"})

	p1, _ := influx.NewPoint("views", map[string]string{"ad_id": "ad2", "media_id": "media1"}, map[string]interface{}{"cpc": 125, "views": 700})
	p2, _ := influx.NewPoint("views", map[string]string{"ad_id": "ad2", "media_id": "media2"}, map[string]interface{}{"cpc": 8000, "views": 500})
	bp.AddPoint(p1)
	bp.AddPoint(p2)

	err = clientMock.Write(bp)
	if err != nil {
		panic(err)
	}

	//Build some queries using query builder
	viewsQuery := inflx.NewQuery("ad_network", "", "views").
		Fill(`0`)
	viewsQuery.
		DataSet("views", "views")

	clickQuery := inflx.NewQuery("ad_network", "", "clicks").
		GroupBy(`"ad_id","time(10w)"`).
		Fill(`0`)
	clickQuery.
		DataSet("clicks", "clicks")

	//Running queries using mock influx client
	res, err := qr.Do(viewsQuery)

	if err != nil {
		panic(err)
	}

	//Running queries using mock influx client
	res2, err := qr.Do(clickQuery)

	if err != nil {
		panic(err)
	}

	//Note that fill the influx mockClient's measurements with rows you intend your query on that measurement returns.
	// Influx client mock doesn't understand your queries. it just figures out what measurement you're querying.
	fmt.Println("Result of views query:", res)
	fmt.Println("Result of clicks query:", res2)

	//And you can use this
	joinedResult, err := inflx.Join(res[0], res2[0], []string{"ad_id"}, []string{"time"}, "nil-field", "nil-tag")

	if err != nil {
		panic(err)
	}
	fmt.Println("Joined result: ", joinedResult)
}
