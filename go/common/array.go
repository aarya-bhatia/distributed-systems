package common

// Remove element at index i from slice and return new slice
func RemoveIndex[T any](arr []T, i int) []T {
	if i < 0 || i >= len(arr) {
		return arr
	}

	n := len(arr)
	arr[i], arr[n-1] = arr[n-1], arr[i]
	return arr[:n-1]
}

// Remove element if exists and return new slice
func RemoveElement[T comparable](array []T, target T) []T {
	for i, element := range array {
		if element == target {
			n := len(array)
			array[i], array[n-1] = array[n-1], array[i]
			return array[:n-1]
		}
	}

	return array
}

// Add element if not exists and return new slice
func AddElement[T comparable](array []T, target T) []T {
	if !HasElement(array, target) {
		array = append(array, target)
	}

	return array
}

func HasElement[T comparable](array []T, target T) bool {
	for _, element := range array {
		if element == target {
			return true
		}
	}
	return false
}

// Remove elements in target from array
func Subtract[T comparable](array []T, targets []T) []T {
	res := []T{}
	set := MakeSet(targets)
	for _, e := range array {
		if _, ok := set[e]; !ok {
			res = append(res, e)
		}
	}
	return res
}

func Intersect[T comparable](a []T, b []T) []T {
	res := []T{}
	set := MakeSet(b)
	for _, e := range a {
		if _, ok := set[e]; ok {
			res = append(res, e)
		}
	}
	return res
}
