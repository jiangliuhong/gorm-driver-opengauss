package postgres

import (
	"errors"
	pq "gitee.com/opengauss/openGauss-connector-go-pq"
)

var errCodes = map[string]string{
	"unique_violation": "23505",
}

func (dialector Dialector) Translate(err error) error {
	if pgErr, ok := err.(*pq.Error); ok {
		if pgErr.Code.String() == errCodes["unique_violation"] {
			return errors.New("Err Duplicated Key")
		}
	}

	return err
}
