package influxqb

import (
	"strings"

	"github.com/influxdata/influxdb/models"
)

func NewQuery(database, retentionPolicy, measurement string) *queryBuilder {
	q := queryBuilder{}
	q.database = database
	q.From(retentionPolicy, measurement)
	q.summaries = map[string]string{}
	q.dataSets = map[string]string{}
	return &q
}

type queryBuilder struct {
	database    string
	measurement string
	timeRange   string
	fill        string
	groupBy     string
	where       []string
	sum         models.Row
	dataSets    map[string]string
	summaries   map[string]string
	response    *queryResult
}

func (query *queryBuilder) String() string {
	var queries []string
	for key, field := range query.dataSets {
		queries = append(queries, query.buildQuery(map[string]string{key: field}, query.groupBy))
	}
	if len(query.summaries) > 0 {
		queries = append(queries, query.buildQuery(query.summaries, ""))
	}
	return strings.Join(queries, ";\n")
}

func (query *queryBuilder) buildQuery(set map[string]string, groupBy string) string {
	var selects []string
	for key, filed := range set {
		sel := filed
		if key != "" {
			sel += " as " + key
		}
		selects = append(selects, sel)
	}
	q := `select ` + strings.Join(selects, ", ") + ` from ` + query.measurement
	if query.where != nil {
		q += ` where ` + strings.Join(query.where, " and ")
	}
	if groupBy != "" {
		q += ` group by ` + groupBy
	}
	if query.fill != "" {
		q += ` fill(` + query.fill + ")"
	}
	return q
}

func (query *queryBuilder) From(rpName, measurement string) *queryBuilder {
	if rpName != "" {
		rpName += "."
	}

	query.measurement = rpName + measurement
	return query
}

func (query *queryBuilder) DataSet(as, field string) *queryBuilder {
	query.dataSets[as] = field
	return query
}

func (query *queryBuilder) Summary(as, field string) *queryBuilder {
	query.summaries[as] = field
	return query
}

func (query *queryBuilder) GroupBy(s string) *queryBuilder {
	query.groupBy = " " + s + " "
	return query
}

func (query *queryBuilder) GroupMinutely(rpName string) *queryBuilder {
	query.From(rpName, "minutely.statistics")
	query.groupBy = " time(60s) "
	return query
}

func (query *queryBuilder) GroupHourly(rpName string) *queryBuilder {
	query.From(rpName, "hourly.statistics")
	query.groupBy = " time(1h) "
	return query
}

func (query *queryBuilder) GroupDaily(rpName string) *queryBuilder {
	query.From(rpName, "daily.statistics")
	query.groupBy = " time(1d) "
	return query
}

func (query *queryBuilder) GroupMonthly(rpName string) *queryBuilder {
	query.From(rpName, "daily.statistics")
	query.groupBy = " time(30d) "
	return query
}

func (query *queryBuilder) GroupYearly(rpName string) *queryBuilder {
	query.From(rpName, "daily.statistics")
	query.groupBy = " time(365d) "
	return query
}

func (query *queryBuilder) Fill(s string) *queryBuilder {
	query.fill = s
	return query
}

func (query *queryBuilder) Where(s string) *queryBuilder {
	query.where = append(query.where, " "+s+" ")
	return query
}
