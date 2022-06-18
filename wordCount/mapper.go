package wordCount

import (
	"map-reduce/common"
	"strings"
	"unicode"
)

func wordCountMapF(document string, value string) (res []common.KeyValue) {
	words := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	for _, word := range words {
		res = append(res, common.KeyValue{Key: word, Value: "1"})
	}
	return res
}
