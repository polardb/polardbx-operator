package json

import "encoding/json"

func Convert2JsonString(obj interface{}) string {
	if obj == nil {
		return ""
	}
	bytes, err := json.Marshal(obj)
	if err != nil {
		return ""
	}
	return string(bytes)
}
