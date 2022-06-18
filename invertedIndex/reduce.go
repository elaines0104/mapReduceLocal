package invertedIndex

import (
	"strconv"
	"strings"
)

func invertedIndexReduceF(key string, values []string) string {
	var fileNames []string

	for _, v := range values {
		if !contains(fileNames, v) {
			fileNames = append(fileNames, v)
		}
	}

	return strconv.Itoa(len(fileNames)) + " " + strings.Join(fileNames, ",")
}
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
