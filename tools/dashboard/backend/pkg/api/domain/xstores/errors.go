package xstores

import "errors"

// Domain error placeholder: Unified error semantics for easy util layer mapping.
var (
	ErrInvalidArgument = errors.New("xstores: invalid argument")
)
