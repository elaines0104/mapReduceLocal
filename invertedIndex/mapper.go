package invertedIndex

import (
	"map-reduce/common"
	"path"
	"strings"
	"unicode"
)

func invertedIndexMapF(document string, value string) (res []common.KeyValue) {
	words := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	for _, word := range words {
		res = append(res, common.KeyValue{Key: word, Value: path.Base(document)})
	}
	return res
}
