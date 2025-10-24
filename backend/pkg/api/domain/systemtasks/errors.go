package systemtasks

import "errors"

// 领域错误占位：统一错误语义。
var (
	ErrInvalidArgument = errors.New("systemtasks: invalid argument")
)
