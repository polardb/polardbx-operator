package systemtasks

import "errors"

// Domain error placeholders: unify error semantics.
var (
	ErrInvalidArgument = errors.New("systemtasks: invalid argument")
)
