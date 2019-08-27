package main

import (
	"fmt"
	"github.com/mdaliyan/influxqb/inflx"
	"github.com/mdaliyan/influxqb/qb"
	"github.com/mdaliyan/influxqb/qr"
)

func main() {

	inflx.Client = inflx.NewClient("http://localhost:8086", "", "")
	publishersQuery1 := qb.NewQuery("sanjagh", "", "publishers").
		Where(`time > now()- 10d`).
		//GroupBy(`time(1d)`).
		GroupBy(`"id"`).
		Fill(`0`)

	publishersQuery1.
		DataSet("clicks1", "sum(click)").
		DataSet("views1", "sum(permutation_view)").
		DataSet("unacceptable_clicks1", "sum(bot_click) + sum(fraud) + sum(duplicate)").
		DataSet("network_income1", "sum(network_income)")

	publishersQuery1.
		Summary("clicks1", "sum(click)").
		DataSet("views1", "sum(permutation_view)").
		DataSet("network_income1", "sum(network_income)")

	publishersQuery2 := qb.NewQuery("sanjagh", "", "publishers").
		Where(`id='id8'`).
		Where(`time > now()- 1w`).
		GroupBy(`time(1d)`).
		Fill(`0`)

	publishersQuery2.
		DataSet("clicks", "sum(click)").
		DataSet("views", "sum(permutation_view)").
		DataSet("publisher_income", "sum(income)").
		DataSet("unacceptable_clicks", "sum(bot_click) + sum(fraud) + sum(duplicate)")

	publishersQuery2.
		Summary("clicks", "sum(click)").
		Summary("unacceptable_clicks", "sum(bot_click) + sum(fraud) + sum(duplicate)").
		Summary("publisher_income", "sum(income)")

	employeesQuery1 := qb.NewQuery("sanjagh", "", "employees").
		Where(`time > now()- 10d`).
		GroupBy(`time(1d)`).
		Fill(`0`)

	employeesQuery1.
		DataSet("salary_avg", "sum(salary)/count(salary)").
		DataSet("hr_salary_budget", "sum(salary)").
		DataSet("minimum_salary", "min(salary)")

	employeesQuery1.
		Summary("salary_avg", "sum(salary)/count(salary)").
		Summary("hr_salary_budget", "sum(salary)").
		Summary("minimum_salary", "min(salary)")

	fmt.Println(qr.ExecuteQueries(employeesQuery1, publishersQuery2, publishersQuery1).String())

}
