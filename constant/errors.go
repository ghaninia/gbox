package constant

import "errors"

var (
	ErrProviderNotFound = errors.New("provider not found for the given driver name")
)
