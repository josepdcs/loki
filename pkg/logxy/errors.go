package logxy

import "github.com/pkg/errors"

var ErrNotShardeableReq = errors.New("Request not shareable")
var ErrNotShardeableQuery = func(query string) error {
	return errors.New("Query not shareable: " + query)
}
