package influxqb

import (
	"errors"
	influx "github.com/influxdata/influxdb1-client/v2"
)

func NewHTTPClient(Host, Username, Password string) influx.Client {
	var err error
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     Host,
		Username: Username,
		Password: Password,
	})
	if err != nil {
		panic(errors.New("influx connection: " + err.Error()))
	}
	return c
}
