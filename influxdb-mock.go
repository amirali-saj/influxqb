package influxqb

import (
	"github.com/influxdata/influxdb1-client/models"
	influx "github.com/influxdata/influxdb1-client/v2"
	"strings"
	"time"
)

type influxClientMock struct {
	dbs map[string]*influxDbMock
}

type influxDbMock struct {
	measurements map[string]*influxMeasurementMock
}
type influxMeasurementMock struct {
	rows []models.Row
}

func NewInfluxClientMock() influxClientMock {
	cl := influxClientMock{}
	cl.Init()
	return cl
}

func (icm *influxClientMock) Init() {
	icm.dbs = make(map[string]*influxDbMock)
}

func (icm *influxClientMock) CreateDb(dbName string) {
	db := &influxDbMock{measurements: make(map[string]*influxMeasurementMock)}
	icm.dbs[dbName] = db
}

func (icm *influxClientMock) Ping(timeout time.Duration) (time.Duration, string, error) {
	return 0 * time.Second, "1.7.7", nil
}

func (icm *influxClientMock) Query(q influx.Query) (*influx.Response, error) {
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

func (idm *influxDbMock) CreateMeasurement(measurementName string) {
	measurement := &influxMeasurementMock{rows: make([]models.Row, 0)}
	idm.measurements[measurementName] = measurement
}
func (imm *influxMeasurementMock) AddRow(row models.Row) {
	imm.rows = append(imm.rows, row)
}

func (icm *influxClientMock) CreateMeasurement(dbName, measurementName string) {
	icm.dbs[dbName].CreateMeasurement(measurementName)
}

func (icm *influxClientMock) AddRowToMeasurement(dbName, measurementName string, row models.Row) {
	icm.dbs[dbName].measurements[measurementName].AddRow(row)
}

func (icm *influxClientMock) Write(bp influx.BatchPoints) error {
	for _, point := range bp.Points() {
		var pointWritten bool
		fieldMap, err := point.Fields()
		if err != nil {
			return err
		}
		columns := make([]string, 0)
		value := make([]interface{}, 0)
		for k, v := range fieldMap {
			columns = append(columns, k)
			value = append(value, v)
		}
		newRow := models.Row{Tags: point.Tags(), Columns: columns}
		for i, row := range icm.dbs[bp.Database()].measurements[point.Name()].rows {
			if row.SameSeries(&newRow) {
				icm.dbs[bp.Database()].measurements[point.Name()].rows[i].Values = append(icm.dbs[bp.Database()].measurements[point.Name()].rows[i].Values, value)
				pointWritten = true
				break
			}
		}
		if !pointWritten {
			newRow.Values = make([][]interface{}, 0)
			newRow.Values = append(newRow.Values, value)
			icm.dbs[bp.Database()].measurements[point.Name()].AddRow(newRow)
		}
	}
	return nil
}

func (icm *influxClientMock) QueryAsChunk(q influx.Query) (*influx.ChunkedResponse, error) {
	return nil, nil
}

func (icm *influxClientMock) Close() error {
	return nil
}
