package influxqb

import (
	"github.com/influxdata/influxdb1-client/models"
	influx "github.com/influxdata/influxdb1-client/v2"
	"strings"
	"time"
)

type InfluxClientMock struct {
	dbs map[string]*InfluxDbMock
}

type InfluxDbMock struct {
	measurements map[string]*InfluxMeasurementMock
}
type InfluxMeasurementMock struct {
	rows []models.Row
}

func (icm *InfluxClientMock) Init() {
	icm.dbs = make(map[string]*InfluxDbMock)
}

func (icm *InfluxClientMock) CreateDb(dbName string) {
	db := &InfluxDbMock{measurements: make(map[string]*InfluxMeasurementMock)}
	icm.dbs[dbName] = db
}

func (icm *InfluxClientMock) Ping(timeout time.Duration) (time.Duration, string, error) {
	return 0 * time.Second, "1.7.7", nil
}

func (icm *InfluxClientMock) Query(q influx.Query) (*influx.Response, error) {
	resp := influx.Response{}
	resp.Err = ""
	words := strings.Split(q.Command, " ")

	var getNext bool
	measurement := ""

	for i := range words {
		if getNext {
			if strings.Trim(words[i], " \n\r\t") != "" {
				measurement = strings.Trim(words[i], " \n\r\t")
				break
			}
		}
		if strings.ToLower(words[i]) == "from" {
			getNext = true
		}
	}

	resp.Results = []influx.Result{{Series: icm.dbs[q.Database].measurements[measurement].rows, Err: "", Messages: []*influx.Message{}}}

	return &resp, nil
}

func (idm *InfluxDbMock) CreateMeasurement(measurementName string) {
	measurement := &InfluxMeasurementMock{rows: make([]models.Row, 0)}
	idm.measurements[measurementName] = measurement
}
func (imm *InfluxMeasurementMock) AddRow(row models.Row) {
	imm.rows = append(imm.rows, row)
}

func (icm *InfluxClientMock) CreateMeasurement(dbName, measurementName string) {
	icm.dbs[dbName].CreateMeasurement(measurementName)
}

func (icm *InfluxClientMock) AddRowToMeasurement(dbName, measurementName string, row models.Row) {
	icm.dbs[dbName].measurements[measurementName].AddRow(row)
}

func (icm *InfluxClientMock) Write(bp influx.BatchPoints) error {
	return nil
}

func (icm *InfluxClientMock) QueryAsChunk(q influx.Query) (*influx.ChunkedResponse, error) {
	return nil, nil
}

func (icm *InfluxClientMock) Close() error {
	return nil
}
