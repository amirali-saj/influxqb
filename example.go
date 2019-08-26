package main

import (
	"encoding/json"
	"fmt"
	"github.com/mdaliyan/influxqb/inflx"
	"github.com/mdaliyan/influxqb/qb"
)

func main() {
	inflx.Client = inflx.NewClient("http://localhost:8086", "", "")
	q := qb.NewQuery("sanjagh", "publishers", "").
		Where(`id='id8'`).
		Where(`time > now()- 10d`).
		GroupBy(`time(1d)`).
		Fill(`0`)

	q.
		DataSet("clicks", "sum(click)").
		DataSet("views", "sum(permutation_view)").
		DataSet("network_income", "sum(network_income)").
		DataSet("publisher_income", "sum(income)").
		DataSet("unacceptable_clicks", "sum(bot_click) + sum(fraud) + sum(duplicate)")

	q.
		Summary("clicks", "sum(click)").
		Summary("unacceptable_clicks", "sum(bot_click) + sum(fraud) + sum(duplicate)").
		Summary("publisher_income", "sum(income)")
	//
	//if err := q.Do("sanjagh"); err != nil {
	//	panic(err)
	//}

	b, _ := json.MarshalIndent(q.Export(), "", "  ")
	fmt.Println(string(b))

}
