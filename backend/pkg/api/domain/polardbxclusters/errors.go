package polardbxclusters

import "errors"

// 领域错误占位：统一错误语义，后续可集中映射到 util 层。
var (
	ErrInvalidArgument = errors.New("polardbxclusters: invalid argument")
)
