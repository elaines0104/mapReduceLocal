package common

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

func MapOutputName(jobName string, mapTask int, reduceTask int) string {
	return jobName + "-mapOutput-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func ReduceOutputName(jobName string, reduceTask int) string {
	return jobName + "-reduceOutput-" + strconv.Itoa(reduceTask)
}
func ResultName(jobName string) string {
	return jobName + "-result.txt"
}
func OpenFiles(column *string) []string {
	var files []string

	if column == nil {
		//root := "/path/to/mapReduceLocalmachado-txt/"
		root := "/path/to/mapReduceLocalteste/"
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			files = append(files, path)
			return nil
		})
		if err != nil {
			fmt.Println("error reading input files")
			return nil
		}
		files = files[1:]
		return files
	} else {
		inFile := "/path/to/mapReduceLocalnetflix/netflix_titles.csv"
		files := append(files, inFile)
		return files

	}

}
