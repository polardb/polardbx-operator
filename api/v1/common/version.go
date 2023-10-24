package common

const (
	AnnotationOperatorCreateVersion = "polardbx-operator/create-version"
)

func NeedCheckPodClusterIp(version string) bool {
	return version == ""
}
