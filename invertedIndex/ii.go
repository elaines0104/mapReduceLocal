package invertedIndex

import (
	"fmt"
	"map-reduce/common"
	"map-reduce/shuffleSort"
	"time"
)

func Ii(method string, jobName string, numberOfMapOutput int, path string, column []string) {
	jobName = jobName + "-invertedIndex"

	files := common.OpenFiles(nil)
	if method == "sequential" {
		iiSequential(jobName, files, numberOfMapOutput, path)
	} else if method == "distributed" {
		iiDistributed(jobName, files, numberOfMapOutput, path)
	}
	merge(numberOfMapOutput, jobName)

}
func iiSequential(jobName string, files []string, numberOfMapOutput int, path string) {
	start := time.Now()
	for i, file := range files {
		shuffleSort.DoMapSequential(jobName, i, file, numberOfMapOutput, invertedIndexMapF, path, nil)
	}
	elapsed := time.Since(start)

	fmt.Println("Map phase took:", elapsed)

	start = time.Now()
	for i := 0; i < numberOfMapOutput; i++ {
		shuffleSort.DoReduceSequential(jobName, i, len(files), invertedIndexReduceF, path)
	}
	elapsed = time.Since(start)

	fmt.Println("Reduce phase took:", elapsed)

}

func iiDistributed(jobName string, files []string, numberOfMapOutput int, path string) {
	start := time.Now()
	shuffleSort.DoMapDistributed(jobName, files, numberOfMapOutput, invertedIndexMapF, path, nil)

	elapsed := time.Since(start)

	fmt.Println("Map phase took:", elapsed)

	start = time.Now()
	shuffleSort.DoReduceDistributed(jobName, numberOfMapOutput, len(files), invertedIndexReduceF, path)

	elapsed = time.Since(start)

	fmt.Println("Reduce phase took:", elapsed)

}
