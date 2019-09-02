package influxqb

import (
	"errors"
	"fmt"
	"github.com/influxdata/influxdb1-client/models"
	influx "github.com/influxdata/influxdb1-client/v2"
)

type Result interface {
	Group(tag string) (*groupedResult, error)
	GetMap() (interface{}, error)
	String() string
}

type groupedResult struct {
	Tag       string
	SubGroups map[string]Result
}

type ungroupedResult struct {
	rows []models.Row
}

func (gr *groupedResult) Group(tag string) (*groupedResult, error) {
	fmt.Println("gr:", tag, "gr.tag:", gr.Tag, "sub:", gr.SubGroups)
	for i, res := range gr.SubGroups {
		newGr, err := res.Group(tag)
		if err == nil {
			gr.SubGroups[i] = newGr
		}
	}
	return gr, nil
}

func (gr *groupedResult) GetMap() (interface{}, error) {
	map1 := make(map[string]interface{})
	for k, v := range gr.SubGroups {
		value, err := v.GetMap()
		if err != nil {
			return map1, err
		}
		map1[k] = value
	}
	return map1, nil
}

func (gr *groupedResult) String() string {
	return fmt.Sprintf("{Tag: %v,Subgroups: %v}", gr.Tag, gr.SubGroups)
}

func contains(slice []string, item string) bool {
	for _, sliceItem := range slice {
		if sliceItem == item {
			return true
		}
	}
	return false
}

func (ugr *ungroupedResult) Group(tag string) (*groupedResult, error) {
	fmt.Println("ugr:", "tag:", tag, "rows:", ugr.rows)

	gr := groupedResult{}
	gr.SubGroups = make(map[string]Result)
	gr.Tag = tag

	tagValues := make([]string, 0)
	for i := range ugr.rows {
		value, found := ugr.rows[i].Tags[tag]
		if !found {
			return &groupedResult{}, errors.New("attempt to group Result by invalid tag")
		}
		if !contains(tagValues, value) {
			tagValues = append(tagValues, value)
		}
	}
	for _, tagValue := range tagValues {

		ur := ungroupedResult{}
		ur.rows = make([]models.Row, 0)
		for j := range ugr.rows {
			value, _ := ugr.rows[j].Tags[tag]
			if value == tagValue {
				delete(ugr.rows[j].Tags, tag)
				ur.rows = append(ur.rows, ugr.rows[j])
			}
		}

		gr.SubGroups[tagValue] = &ur
	}

	return &gr, nil
}

func (ugr *ungroupedResult) GetMap() (interface{}, error) {
	return ugr.rows, nil
}

func (ugr *ungroupedResult) String() string {
	return fmt.Sprint("{", ugr.rows, "}")
}

func NewResult(r influx.Result) Result {
	res := ungroupedResult{}
	res.rows = make([]models.Row, 0)

	for _, s := range r.Series {
		row := models.Row{}
		row.Tags = make(map[string]string)
		row.Tags = appendMaps(row.Tags, s.Tags)
		row.Columns = append(s.Columns)
		row.Values = append(s.Values)
		res.rows = append(res.rows, row)
	}
	return &res
}

func appendMaps(map1, map2 map[string]string) map[string]string {
	for k, v := range map2 {
		map1[k] = v
	}
	return map1
}
