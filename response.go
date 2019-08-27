package influxqb

import "github.com/mdaliyan/govert"

type DataSets map[string]DataSet

func (d DataSets) Get(name string) DataSet {
	dataset, ok := d[name]
	if !ok {
		return DataSet{}
	}
	return dataset
}

type DataSet [][]interface{}

func (ds DataSet) Points() (p []interface{}) {
	for _, value := range ds {
		p = append(p, value[1])
	}
	return
}

func (ds DataSet) Point(index int) interface{} {
	if index > len(ds) {
		return nil
	}
	return ds[index]
}

func (ds DataSet) LastPoint() interface{} {
	ind := len(ds)
	if ind == 0 {
		return nil
	}
	return ds[len(ds)-1]
}

type Response struct {
	Summary  map[string]interface{} `json:"summary"`
	DataSets DataSets               `json:"sets"`
}

type Walker func(dataSets DataSets) interface{}

func (r *Response) Calculate(as string, calc Walker) {
	r.Summary[as] = calc(r.DataSets)
}

func (r *Response) Sum(from ...string) float64 {
	var sum float64
	for _, fieldName := range from {
		for _, point := range r.DataSets.Get(fieldName).Points() {
			sum += govert.Float64(point)
		}
	}
	return sum
}

func (r *Response) SetSummary(as string, data interface{}) {
	r.Summary[as] = data
}
