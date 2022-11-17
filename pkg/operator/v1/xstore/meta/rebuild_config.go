package meta

import "github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"

type RebuildConfig struct {
	LogSeparation string
	NodeName      string
}

func (r *RebuildConfig) ComputeHash() string {
	objHash, err := security.HashObj(r)
	if err != nil {
		panic(err)
	}
	return objHash
}
