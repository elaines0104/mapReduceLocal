package netflixdata

import (
	"map-reduce/common"
	"strings"
)

func netflixDataMapF(document string, value string) (res []common.KeyValue) {
	values := strings.Split(value, ",")
	for i := 0; i < len(values); i++ {
		column := values[i]
		res = append(res, common.KeyValue{Key: column, Value: "1"})
	}
	return res
}
