package influxqb

import (
	"errors"
	//"errors"
	"github.com/influxdata/influxdb1-client/models"
	influx "github.com/influxdata/influxdb1-client/v2"
)

func Join(result1, result2 influx.Result, tags, columns []string, fieldNilFiller interface{}, tagNilFiller string) (influx.Result, error) {
	joinedResult := influx.Result{}

	joinedResult.Series = make([]models.Row, 0)

	if len(result1.Series) == 0 || len(result2.Series) == 0 {
		return joinedResult, errors.New("one of inserted results has zero rows")
	}

	//merge columns
	joinedResultColumns := mergeColumns(result1.Series[0].Columns, result2.Series[0].Columns)

	//find indices of columns used for join operation in joinedResult columns
	joinColumnIndices := make([]int, 0)
	for _, col := range columns {
		index, found := findIndex(joinedResultColumns, col)
		if !found {
			return joinedResult, errors.New("invalid columns determined for join operation")
		}
		joinColumnIndices = append(joinColumnIndices, index)
	}

	//Table that shows which two series were merged together. Used for expanding lonely series so they fit result's schema (columns).
	joined := make([][]bool, 0)
	for i := range result1.Series {
		joined = append(joined, make([]bool, 0))
		for range result2.Series {
			joined[i] = append(joined[i], false)
		}
	}
	//AllTags includes at the end merge of tags of both results being joined with nil filler value when expanding lonely series.

	allTags := make(map[string]string)

	//Joins series with matching tags.

	for i, s1 := range result1.Series {
		for j, s2 := range result2.Series {
			if matchTags(s1, s2, tags) {
				joinedResultTags := appendMaps(s1.Tags, s2.Tags)
				allTags = appendMaps(map[string]string{}, joinedResultTags)

				joinedRow := joinRows(s1, s2, joinColumnIndices, joinedResultColumns, joinedResultTags, fieldNilFiller)
				joinedResult.Series = append(joinedResult.Series, joinedRow)
				joined[i][j] = true
			}
		}
	}

	//Joins lonely series.

	for i := range allTags {
		allTags[i] = tagNilFiller
	}
	for i := 0; i < len(result2.Series); i++ {
		isLonely := true
		for j := 0; j < len(joined); j++ {
			if joined[j][i] {
				isLonely = false
				break
			}
		}
		if isLonely {
			joinedResult.Series = append(joinedResult.Series, expandRow(result2.Series[i], joinedResultColumns, allTags, fieldNilFiller))
			continue
		}
	}

	for i := range allTags {
		allTags[i] = tagNilFiller
	}
	for i := 0; i < len(result1.Series); i++ {
		aloneValue := true
		for j := 0; j < len(result2.Series); j++ {
			if joined[i][j] {
				aloneValue = false
				break
			}
		}
		if aloneValue {
			joinedResult.Series = append(joinedResult.Series, expandRow(result1.Series[i], joinedResultColumns, allTags, fieldNilFiller))
			continue
		}
	}

	return joinedResult, nil
}

func joinRows(row1, row2 models.Row, indices []int, columns []string, tags map[string]string, fillerObject interface{}) models.Row {
	joinedRow := models.Row{}
	joinedRow.Tags = tags
	joinedRow.Columns = columns

	values1 := append(row1.Values)
	values2 := append(row2.Values)

	row1Indices := make([]int, len(columns))
	row2Indices := make([]int, len(columns))

	for index := range columns {
		var found bool
		for i := range row1.Columns {
			if columns[index] == row1.Columns[i] {
				row1Indices[index] = i
				found = true
				break
			}
			if !found {
				row1Indices[index] = -1
			}
		}
	}

	for index := range columns {
		var found bool
		for i := range row2.Columns {
			if columns[index] == row2.Columns[i] {
				row2Indices[index] = i
				found = true
				break
			}
			if !found {
				row2Indices[index] = -1
			}
		}
	}

	joined := make([][]bool, 0)
	for i := range row1.Values {
		joined = append(joined, make([]bool, 0))
		for range row2.Values {
			joined[i] = append(joined[i], false)
		}
	}

	for i, v1 := range values1 {
		for j, v2 := range values2 {
			if joined[i][j] {
				continue
			}

			if matchFields(v1, v2, indices) {
				joinedRow.Values = append(joinedRow.Values, mergeValues(v1, v2, indices))
				joined[i][j] = true
			}

		}
		aloneValue := true
		for _, value := range joined[i] {
			if value {
				aloneValue = false
				break
			}
		}
		if aloneValue {
			joinedRow.Values = append(joinedRow.Values, expandValue(v1, columns, row1Indices, fillerObject))
			continue
		}
	}

	for i := 0; i < len(row2.Values); i++ {
		aloneValue := true
		for j := 0; j < len(joined); j++ {
			if joined[j][i] {
				aloneValue = false
				break
			}
		}
		if aloneValue {
			joinedRow.Values = append(joinedRow.Values, expandValue(values2[i], columns, row2Indices, fillerObject))
			continue
		}
	}

	return joinedRow
}

//Expands a row to match desired schema. (columns)

func expandRow(row models.Row, columns []string, allTags map[string]string, fieldFiller interface{}) models.Row {
	expandedRow := models.Row{}
	expandedRow.Tags = appendMaps(allTags, row.Tags)
	expandedRow.Columns = columns
	valueColumnsIndices := make([]int, len(columns))
	for index, col := range columns {
		var found bool
		for i, col2 := range row.Columns {
			if col2 == col {
				valueColumnsIndices[index] = i
				found = true
				break
			}
		}
		if !found {
			valueColumnsIndices[index] = -1
		}

	}
	expandedRow.Values = make([][]interface{}, 0)
	for _, val := range row.Values {
		expandedRow.Values = append(expandedRow.Values, expandValue(val, columns, valueColumnsIndices, fieldFiller))
	}

	return expandedRow
}

//Expands a value to match desired schema. (columns)  (value means result.series[i].values[j])
func expandValue(value []interface{}, columns []string, valueColumnsIndices []int, fillerObject interface{}) []interface{} {
	expandedValue := make([]interface{}, len(columns))
	for index := range columns {
		if valueColumnsIndices[index] == -1 {
			expandedValue[index] = fillerObject
			continue
		}

		expandedValue[index] = value[valueColumnsIndices[index]]
	}
	return expandedValue
}

func mergeValues(value1, value2 []interface{}, indices []int) []interface{} {
	mergedValue := append(value1)

	for i, field := range value2 {
		if !containsInt(indices, i) {
			mergedValue = append(mergedValue, field)
		}
	}
	return mergedValue
}

func containsInt(slice []int, number int) bool {
	for _, element := range slice {
		if element == number {
			return true
		}
	}
	return false
}

func matchFields(value1, value2 []interface{}, indices []int) bool {
	for _, index := range indices {
		if len(value1) <= index || len(value2) <= index {
			continue
		}
		if value1[index] != value2[index] {
			return false
		}
	}
	return true
}

func matchTags(row1, row2 models.Row, tags []string) bool {
	for _, tag := range tags {
		if row1.Tags[tag] != row2.Tags[tag] {
			return false
		}
	}
	return true
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

func findIndex(slice []string, key string) (int, bool) {
	for i := range slice {
		if slice[i] == key {
			return i, true
		}
	}
	return -1, false
}
