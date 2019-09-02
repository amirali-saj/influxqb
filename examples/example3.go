package main

import (
	"fmt"
	"github.com/influxdata/influxdb1-client/models"
	influx "github.com/influxdata/influxdb1-client/v2"
	"github.com/mdaliyan/influxqb"
	inflx "github.com/mdaliyan/influxqb"
	"os"
	"time"
)

func main() {
	cl := influxqb.InfluxClientMock{}
	cl.Init()
	fmt.Println(cl.Ping(25 * time.Second))
	clInterface := influx.Client(&cl)
	qr, err := inflx.NewQueryRunner(&clInterface)

	if err != nil {
		panic(err)
	}

	cl.CreateDb("sanjagh")
	cl.CreateMeasurement("sanjagh", "ads")
	cl.CreateMeasurement("sanjagh", "views")

	//cl.AddRowToMeasurement("sanjagh","ads",models.Row{Tags:map[string]string{"ad_id":"ad1","media_id":"m2"},Columns:[]string {"time","clicks"},Values:[][]interface{}{{"0",0},{"5",5},{"10",8},{"15",30},{"35",351} }})
	cl.AddRowToMeasurement("sanjagh", "ads", models.Row{Tags: map[string]string{"ad_id": "ad2", "media_id": "m2"}, Columns: []string{"time", "clicks"}, Values: [][]interface{}{{"0", 0}, {"5", 25}, {"10", 48}, {"15", 130}, {"45", 451}}})
	cl.AddRowToMeasurement("sanjagh", "ads", models.Row{Tags: map[string]string{"ad_id": "ad3", "media_id": "m3"}, Columns: []string{"time", "clicks"}, Values: [][]interface{}{{"0", 0}, {"5", 100}, {"10", 200}, {"15", 300}, {"55", 551}}})
	cl.AddRowToMeasurement("sanjagh", "ads", models.Row{Tags: map[string]string{"ad_id": "ad4", "media_id": "m5"}, Columns: []string{"time", "clicks"}, Values: [][]interface{}{{"55", 551}}})

	cl.AddRowToMeasurement("sanjagh", "views", models.Row{Tags: map[string]string{"ad_id": "ad1", "owner_id": "o1"}, Columns: []string{"time", "views"}, Values: [][]interface{}{{"0", 3}, {"5", 30}, {"10", 300}, {"15", 3000}, {"30", 301}}})
	cl.AddRowToMeasurement("sanjagh", "views", models.Row{Tags: map[string]string{"ad_id": "ad2", "owner_id": "o2"}, Columns: []string{"time", "views"}, Values: [][]interface{}{{"0", 0}, {"5", 12}, {"10", 140}, {"15", 1600}, {"40", 401}}})
	cl.AddRowToMeasurement("sanjagh", "views", models.Row{Tags: map[string]string{"ad_id": "ad3", "owner_id": "o3"}, Columns: []string{"time", "views"}, Values: [][]interface{}{{"0", 0}, {"5", 20}, {"10", 40}, {"15", 60}, {"50", 501}}})
	adQuery := inflx.NewQuery("sanjagh", "", "ads").
		GroupBy(`"ad_id","media_id","ad_type"`).
		Fill(`0`)
	adQuery.
		DataSet("cpc", "cpc")

	viewQuery := inflx.NewQuery("sanjagh", "", "views").
		GroupBy(`"ad_id","time(10w)"`).
		Fill(`0`)
	viewQuery.
		DataSet("views", "views")

	res, _ := qr.Do(adQuery)
	res2, err := qr.Do(viewQuery)

	//cl.AddRowToMeasurement("sanjagh","ages",models.Row{Tags: map[string]string{"id":"1","name":"ali"},Columns: []string{"time","age"},Values: [][]interface{}{{"0",15},{"1",16}}})
	//cl.AddRowToMeasurement("sanjagh","addresses",models.Row{Tags: map[string]string{"id":"1","last_name":"hasani"},Columns: []string{"time","addr"},Values: [][]interface{}{{"0","Tehran"},{"5","Shiraz"}}})

	//ageQuery := inflx.NewQuery("sanjagh", "", "ages").
	//	GroupBy(`"name","id"`).
	//	Fill(`0`)
	//ageQuery.
	//	DataSet("age", "age")
	//
	//addrQuery := inflx.NewQuery("sanjagh", "", "addresses").
	//	GroupBy(`"id","time(10w)"`).
	//	Fill(`0`)
	//addrQuery.
	//	DataSet("addresses", "addresses")

	//res, _ := qr.Do(ageQuery)
	//res2, err := qr.Do(addrQuery)
	if err != nil {
		panic(err)
	}

	fmt.Println("(((", res, ")))")

	fmt.Println("(((", res2, ")))")

	//for i := range res[0].Series {
	//	for j := range res[0].Series[i].Values {
	//		res[0].Series[i].Values[j][0] = "0"
	//	}
	//}
	//for i := range res2[0].Series {
	//	for j := range res2[0].Series[i].Values {
	//		res2[0].Series[i].Values[j][0] = "0"
	//	}
	//}

	a, e := inflx.Join(res[0], res2[0], []string{"ad_id"}, []string{"time"}, "nil-field", "nil-tag")
	//a,_ = inflx.Join(res[0],a,[]string{"ad_id"},[]string{"time"},"nil-field","nil-tag")

	fmt.Println("q", a, e, "q")

	os.Exit(1)
	//results := inflx.NewResult(res)
	////
	//
	//by_media, err := results.Group("media_id")
	//fmt.Println(by_media)
	//y, err := by_media.Group("ad_id")
	//fmt.Println(y)
	//mInterface, err := y.GetMap()
	//if err != nil {
	//	panic(err)
	//}
	//m := mInterface.(map[string]interface{})
	//fmt.Println(m["media1"].(map[string]interface{})["ad1"].([]models.Row)[0].Tags["ad_type"])
}
