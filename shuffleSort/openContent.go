package shuffleSort

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func getContent(inFile string, column *string) string {
	if column != nil {
		return readCSVFile(inFile, *column)
	} else {

		content, err := ioutil.ReadFile(inFile)
		if err != nil {
			fmt.Println(err)
		}
		return string(content)
	}
}

func readCSVFile(filePath string, column string) string {
	isFirstRow := true
	headerMap := make(map[string]int)

	var lines []string
	f, err := os.Open(filePath)

	if err != nil {

		fmt.Println(err)
		return ""
	}

	r := csv.NewReader(f)
	for {
		record, err := r.Read()
		if err != nil {
			break
		}

		// Handle first row case
		if isFirstRow {
			isFirstRow = false

			// Add mapping: Column/property name --> record index
			for i, v := range record {
				headerMap[v] = i
			}

			continue
		}

		s := strings.Split(record[headerMap[column]], ",")

		for i := 0; i < len(s); i++ {
			if s[i] != "" {
				lines = append(lines, strings.TrimSpace(s[i]))
			}
		}

	}

	return strings.Join(lines, ",")
}
