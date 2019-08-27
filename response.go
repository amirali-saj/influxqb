package influxqb

import "github.com/mdaliyan/govert"

type dataSets map[string]dataSet

func (d dataSets) Get(name string) dataSet {
	dataset, ok := d[name]
	if !ok {
		return dataSet{}
	}
	return dataset
}

type dataSet [][]interface{}

func (ds dataSet) Points() (p []interface{}) {
	for _, value := range ds {
		p = append(p, value[1])
	}
	return
}

func (ds dataSet) Point(index int) interface{} {
	if index > len(ds) {
		return nil
	}
	return ds[index]
}

func (ds dataSet) LastPoint() interface{} {
	ind := len(ds)
	if ind == 0 {
		return nil
	}
	return ds[len(ds)-1]
}

type response struct {
	Summary  map[string]interface{} `json:"summary"`
	DataSets dataSets               `json:"sets"`
}

type walker func(interface{}) interface{}

func (r *response) Walk(as string, calc walker) {
	for _, dataset := range r.DataSets {
		r.Summary[as] = calc(dataset)
	}
}

func (r *response) Sum(from ...string) float64 {
	var sum float64
	for _, fieldName := range from {
		for _, point := range r.DataSets.Get(fieldName).Points() {
			sum += govert.Float64(point)
		}
	}
	return sum
}

func (r *response) Count(from ...string) int {
	var count int
	for _, fieldName := range from {
		for range r.DataSets.Get(fieldName).Points() {
			count++
		}
	}
	return count
}

func (r *response) SetSummary(as string, data interface{}) {
	r.Summary[as] = data
}
