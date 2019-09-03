package main

import (
	"fmt"
	"github.com/influxdata/influxdb1-client/models"
	inflx "github.com/mdaliyan/influxqb"
	"time"
)

func main() {
	cl := inflx.NewHTTPClient("http://localhost:8086", "", "")
	fmt.Println(cl.Ping(3 * time.Second))
	qr, err := inflx.NewQueryRunner(&cl)
	if err != nil {
		panic(err)
	}
	adQuery := inflx.NewQuery("sanjagh", "", "ads").
		GroupBy(`"ad_id","media_id","ad_type"`).
		Fill(`0`)
	adQuery.
		DataSet("cpc", "cpc")

	viewQuery := inflx.NewQuery("sanjagh", "", "views").
		GroupBy(`"ad_id",time(10w)`).
		Fill(`0`)
	viewQuery.
		DataSet("views", "sum(views)/count(views)")

	res, _ := qr.Do(adQuery)
	res2, err := qr.Do(viewQuery)
	if err != nil {
		panic(err)
	}
	for i := range res[0].Series {
		fmt.Println(res[0].Series[i].Columns, res[0].Series[i].Values)
	}

	for i := range res2[0].Series {
		fmt.Println(res2[0].Series[i].Columns, res2[0].Series[i].Values)
	}

	for i := range res[0].Series {
		for j := range res[0].Series[i].Values {
			res[0].Series[i].Values[j][0] = j % 5
		}
	}

	for i := range res2[0].Series {
		for j := range res2[0].Series[i].Values {
			res2[0].Series[i].Values[j][0] = j % 5
		}
	}

	//Joining two results using time field and ad_id. If some rows don't participate in join operation are added to the result with nilFiller
	//values as absent field, or tag values.
	joined, err := inflx.Join(res[0], res2[0], []string{"ad_id"}, []string{"time"}, "nil-field", "nil-tag")
	if err != nil {
		panic(err)
	}

	var results = inflx.NewResult(joined)

	//Grouping result using media_id
	byMedia, err := results.Group("media_id")
	fmt.Println(byMedia)
	if byMedia == nil {
		panic("byMedia is nil!")
	}
	//Grouping each sub result using ad_id
	byMediaThenByAdId, err := byMedia.Group("ad_id")
	fmt.Println(byMediaThenByAdId)
	if byMediaThenByAdId == nil {
		panic("byMediaThenByAdId is nil!")
	}

	//Use this to export result as a nested map structure.
	mapInterface, err := byMediaThenByAdId.GetMap()
	if err != nil {
		panic(err)
	}
	m := mapInterface.(map[string]interface{})

	//An example of using exported nested structure.
	fmt.Println(m["media1"].(map[string]interface{})["ad1"].([]models.Row)[0].Tags["ad_type"])
}
