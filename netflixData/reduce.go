package netflixdata

import (
	"strconv"
)

func netflixDataReduceF(key string, values []string) string {
	total := 0
	for _, v := range values {
		val, _ := strconv.Atoi(v)
		total += val
	}
	return strconv.Itoa(total)
}
