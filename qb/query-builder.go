package qb

import (
	"strings"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb1-client/v2"
)

func NewQuery(database, retentionPolicy, measurement string) *HistogramBuilder {
	h := HistogramBuilder{}
	h.Database = database
	h.From(retentionPolicy, measurement)
	h.summaries = map[string]string{}
	h.dataSets = map[string]string{}
	return &h
}

type HistogramBuilder struct {
	Database    string
	Measurement string
	timeRange   string
	fill        string
	groupBy     string
	where       []string
	sum         models.Row
	dataSets    map[string]string
	summaries   map[string]string
	Response    *Indexer
	RawResponse *client.Response
}

func (h *HistogramBuilder) Export() (r Response) {
	r = Response{
		Summary:  h.Response.GetSummary(),
		DataSets: DataSets{},
	}
	for key := range h.dataSets {
		r.DataSets[key] = h.Response.GetTimeSeriesFor(key)
	}
	return
}

func (h *HistogramBuilder) Query() string {
	var queries []string
	for key, field := range h.dataSets {
		queries = append(queries, h.buildQuery(map[string]string{key: field}, h.groupBy))
	}
	if len(h.summaries) > 0 {
		queries = append(queries, h.buildQuery(h.summaries, ""))
	}
	return strings.Join(queries, ";\n")
}

func (h *HistogramBuilder) buildQuery(set map[string]string, groupBy string) string {
	var selects []string
	for key, filed := range set {
		sel := filed
		if key != "" {
			sel += " as " + key
		}
		selects = append(selects, sel)
	}
	q := `select ` + strings.Join(selects, ", ") + ` from ` + h.Measurement
	if h.where != nil {
		q += ` where ` + strings.Join(h.where, " and ")
	}
	if groupBy != "" {
		q += ` group by ` + groupBy
	}
	if h.fill != "" {
		q += ` fill(` + h.fill + ")"
	}
	return q
}

func (h *HistogramBuilder) From(rpName, measurement string) *HistogramBuilder {
	if rpName != "" {
		rpName += "."
	}

	h.Measurement = rpName + measurement
	return h
}

func (h *HistogramBuilder) DataSet(as, field string) *HistogramBuilder {
	h.dataSets[as] = field
	return h
}

func (h *HistogramBuilder) Summary(as, field string) *HistogramBuilder {
	h.summaries[as] = field
	return h
}

func (h *HistogramBuilder) GroupBy(s string) *HistogramBuilder {
	h.groupBy = " " + s + " "
	return h
}

func (h *HistogramBuilder) GroupMinutely(rpName string) *HistogramBuilder {
	h.From(rpName, "minutely.statistics")
	h.groupBy = " time(60s) "
	return h
}

func (h *HistogramBuilder) GroupHourly(rpName string) *HistogramBuilder {
	h.From(rpName, "hourly.statistics")
	h.groupBy = " time(1h) "
	return h
}

func (h *HistogramBuilder) GroupDaily(rpName string) *HistogramBuilder {
	h.From(rpName, "daily.statistics")
	h.groupBy = " time(1d) "
	return h
}

func (h *HistogramBuilder) GroupMonthly(rpName string) *HistogramBuilder {
	h.From(rpName, "daily.statistics")
	h.groupBy = " time(30d) "
	return h
}

func (h *HistogramBuilder) GroupYearly(rpName string) *HistogramBuilder {
	h.From(rpName, "daily.statistics")
	h.groupBy = " time(365d) "
	return h
}

func (h *HistogramBuilder) Fill(s string) *HistogramBuilder {
	h.fill = s
	return h
}

func (h *HistogramBuilder) Where(s string) *HistogramBuilder {
	h.where = append(h.where, " "+s+" ")
	return h
}
