package influxqb

import (
	"errors"
	//"errors"
	"github.com/influxdata/influxdb1-client/models"
	influx "github.com/influxdata/influxdb1-client/v2"
)

func Join(result1, result2 influx.Result, tags, columns []string) (influx.Result, error) {

	joinedResult := influx.Result{}

	joinedResult.Series = make([]models.Row, 0)
	if len(result1.Series) == 0 || len(result2.Series) == 0 {
		return joinedResult, errors.New("one of inserted results has zero rows")
	}

	joinedResultColumns := mergeColumns(result1.Series[0].Columns, result2.Series[0].Columns)
	joinedResultTags := appendMaps(result1.Series[0].Tags, result2.Series[0].Tags)

	joinColumnIndices := make([]int, 0)
	for _, col := range columns {
		index, found := findIndex(joinedResultColumns, col)
		if !found {
			return joinedResult, errors.New("invalid columns determined for join operation")
		}
		joinColumnIndices = append(joinColumnIndices, index)
	}

	series1 := append(result1.Series)
	series2 := append(result2.Series)

	for i, s1 := range series1 {
		for j, s2 := range series2 {
			if matchTags(s1, s2, tags) {
				joinedRow, success := joinRows(s1, s2, joinColumnIndices, joinedResultColumns, joinedResultTags)
				if success {
					joinedResult.Series = append(joinedResult.Series, joinedRow)

					series1[i].Name = "<used>"
					series2[j].Name = "<used>"
				}
			}
		}
	}

	for _, s1 := range series2 {
		if s1.Name == "<used>" {
			continue
		}
		joinedResult.Series = append(joinedResult.Series, expandRow(s1, joinedResultColumns, joinedResultTags))
	}

	for _, s2 := range series2 {
		if s2.Name == "<used>" {
			continue
		}
		joinedResult.Series = append(joinedResult.Series, expandRow(s2, joinedResultColumns, joinedResultTags))
	}
	return joinedResult, nil
}

func joinRows(row1, row2 models.Row, indices []int, columns []string, tags map[string]string) (models.Row, bool) {
	joinedRow := models.Row{}
	joinedRow.Tags = tags
	joinedRow.Columns = columns
	for _, ts1 := range row1.Values {
		for _, ts2 := range row2.Values {
			for _, index := range indices {
				if ts1[index] != ts2[index] {
					return joinedRow, false
				}
			}
			joinedRow.Values = append(joinedRow.Values, mergeInterfaceSlices(ts1, ts2))
		}
	}
	return joinedRow, true
}

func matchTags(row1, row2 models.Row, tags []string) bool {
	for _, tag := range tags {
		if row1.Tags[tag] != row2.Tags[tag] {
			return false
		}
	}
	return true
}

func expandRow(row1 models.Row, columns []string, tags map[string]string) models.Row {
	joinedRow := models.Row{}
	joinedRow.Tags = tags
	joinedRow.Columns = columns
	nilIndices := make([]int, 0)
	for i, col := range columns {
		if !contains(row1.Columns, col) {
			nilIndices = append(nilIndices, i)
		}
	}
	for _, ts1 := range row1.Values {
		for _, index := range nilIndices {
			ts1[index] = 0
		}
		joinedRow.Values = append(joinedRow.Values, ts1)
	}
	return joinedRow
}

func mergeColumns(columns1, columns2 []string) []string {
	newColumns := append(columns1)
	for _, col2 := range columns2 {
		if !contains(columns1, col2) {
			newColumns = append(newColumns, col2)
		}
	}
	return newColumns
}

func mergeInterfaceSlices(columns1, columns2 []interface{}) []interface{} {
	newColumns := append(columns1)
	for _, col2 := range columns2 {
		if !containsInterface(columns1, col2) {
			newColumns = append(newColumns, col2)
		}
	}
	return newColumns
}

func findIndex(slice []string, key string) (int, bool) {
	for i := range slice {
		if slice[i] == key {
			return i, true
		}
	}
	return -1, false
}
