package influxqb

import (
	"strings"

	"github.com/influxdata/influxdb/models"
)

func NewQuery(database, retentionPolicy, measurement string) *QueryBuilder {
	q := QueryBuilder{}
	q.database = database
	q.From(retentionPolicy, measurement)
	q.summaries = map[string]string{}
	q.dataSets = map[string]string{}
	return &q
}

type QueryBuilder struct {
	database    string
	measurement string
	timeRange   string
	fill        string
	groupBy     string
	where       []string
	sum         models.Row
	dataSets    map[string]string
	summaries   map[string]string
	response    *Indexer
}

func (query *QueryBuilder) Query() string {
	var queries []string
	for key, field := range query.dataSets {
		queries = append(queries, query.buildQuery(map[string]string{key: field}, query.groupBy))
	}
	if len(query.summaries) > 0 {
		queries = append(queries, query.buildQuery(query.summaries, ""))
	}
	return strings.Join(queries, ";\n")
}

func (query *QueryBuilder) buildQuery(set map[string]string, groupBy string) string {
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

func (query *QueryBuilder) From(rpName, measurement string) *QueryBuilder {
	if rpName != "" {
		rpName += "."
	}

	query.measurement = rpName + measurement
	return query
}

func (query *QueryBuilder) DataSet(as, field string) *QueryBuilder {
	query.dataSets[as] = field
	return query
}

func (query *QueryBuilder) Summary(as, field string) *QueryBuilder {
	query.summaries[as] = field
	return query
}

func (query *QueryBuilder) GroupBy(s string) *QueryBuilder {
	query.groupBy = " " + s + " "
	return query
}

func (query *QueryBuilder) GroupMinutely(rpName string) *QueryBuilder {
	query.From(rpName, "minutely.statistics")
	query.groupBy = " time(60s) "
	return query
}

func (query *QueryBuilder) GroupHourly(rpName string) *QueryBuilder {
	query.From(rpName, "hourly.statistics")
	query.groupBy = " time(1h) "
	return query
}

func (query *QueryBuilder) GroupDaily(rpName string) *QueryBuilder {
	query.From(rpName, "daily.statistics")
	query.groupBy = " time(1d) "
	return query
}

func (query *QueryBuilder) GroupMonthly(rpName string) *QueryBuilder {
	query.From(rpName, "daily.statistics")
	query.groupBy = " time(30d) "
	return query
}

func (query *QueryBuilder) GroupYearly(rpName string) *QueryBuilder {
	query.From(rpName, "daily.statistics")
	query.groupBy = " time(365d) "
	return query
}

func (query *QueryBuilder) Fill(s string) *QueryBuilder {
	query.fill = s
	return query
}

func (query *QueryBuilder) Where(s string) *QueryBuilder {
	query.where = append(query.where, " "+s+" ")
	return query
}
