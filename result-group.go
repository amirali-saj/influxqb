package influxqb

import (
	"errors"
	"fmt"
	influx "github.com/influxdata/influxdb1-client/v2"
)

type Result interface {
	Group(tag string) (*groupedResult, error)
	String() string
}

type groupedResult struct {
	Tag       string
	SubGroups map[string]Result
}

type ungroupedResult struct {
	rows []map[string]string
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
		value, found := ugr.rows[i][tag]
		if !found {
			return &groupedResult{}, errors.New("attempt to group Result by invalid tag")
		}
		if !contains(tagValues, value) {
			tagValues = append(tagValues, value)
		}
	}
	for _, tagValue := range tagValues {

		ur := ungroupedResult{}
		ur.rows = make([]map[string]string, 0)
		for j := range ugr.rows {
			value, _ := ugr.rows[j][tag]
			if value == tagValue {
				delete(ugr.rows[j], tag)
				ur.rows = append(ur.rows, ugr.rows[j])
			}
		}

		gr.SubGroups[tagValue] = &ur
	}

	return &gr, nil
}

func (ugr *ungroupedResult) String() string {
	return fmt.Sprint("{", ugr.rows, "}")
}

func NewResult(r []influx.Result) Result {
	res := ungroupedResult{}
	res.rows = make([]map[string]string, 0)

	for _, s := range r[0].Series {
		res.rows = append(res.rows, s.Tags)
	}
	return &res
}

//func (rg *resultGroup) Get(key string) (Result resultGroup, found bool) {
//	Result, found = rg.SubGroups[key]
//	return
//}

//func (rg *resultGroup) AddSubGroup(name string, subGroup resultGroup) {
//	rg.SubGroups[name] = subGroup
//}

//func (rg *resultGroup) ListSubGroups() (list []string) {
//
//	for k := range rg.SubGroups {
//		list = append(list, k)
//	}
//	return
//}
