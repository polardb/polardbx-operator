package xstores

import "errors"

// 领域错误占位：统一错误语义，便于 util 层映射。
var (
	ErrInvalidArgument = errors.New("xstores: invalid argument")
)
