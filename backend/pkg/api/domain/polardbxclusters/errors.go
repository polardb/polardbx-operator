package polardbxclusters

import "errors"

// Domain error placeholders: unify error semantics; may later be centrally mapped to util layer.
var (
	ErrInvalidArgument = errors.New("polardbxclusters: invalid argument")
)
