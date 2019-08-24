package main

import (
	"encoding/json"
	"fmt"
	"github.com/mdaliyan/influxqb/qb"
)

func main() {

	stats := qb.NewHistogram("statistics").
		Where(`publisher.id = 'e8001a50-2369-4313-9664-4e91b4f827a6'`).
		Where(`time > now() - 10d`).
		GroupBy(`time(1d)`).
		Fill(`0`)

	stats.
		DataSet("clicks", "sum(click)").
		DataSet("views", "sum(permutation_view)").
		DataSet("network_income", "sum(network.income)").
		DataSet("publisher_income", "sum(publisher.income)").
		DataSet("unacceptable_clicks", "sum(bot_click) + sum(fraud) + sum(duplicate)")

	stats.
		Summary("clicks", "sum(click)").
		Summary("unacceptable_clicks", "sum(bot_click) + sum(fraud) + sum(duplicate)").
		Summary("publisher_income", "sum(publisher.income)")

	if err := stats.Do("sanjagh"); err != nil {
		panic(err)
	}

	b, _ := json.MarshalIndent(stats.Export(), "", "  ")
	fmt.Println(string(b))

}
