package slice

func NotIn[T comparable](element T, candidates ...T) bool {
	for _, candidate := range candidates {
		if element == candidate {
			return false
		}
	}
	return true
}
