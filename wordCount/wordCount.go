package wordCount

import (
	"fmt"
	"map-reduce/common"
	shuffleSort "map-reduce/shuffleSort"
	"time"
)

func WordCount(method string, jobName string, numberOfMapOutput int, path string, column []string) {
	files := common.OpenFiles(nil)

	jobName = jobName + "-WordCount"
	if method == "sequential" {
		wordCountSequential(jobName, files, numberOfMapOutput, path)
	} else if method == "distributed" {
		wordCountDistributed(jobName, files, numberOfMapOutput, path)
	}
	merge(numberOfMapOutput, jobName)
}
func wordCountSequential(jobName string, files []string, numberOfMapOutput int, path string) {
	start := time.Now()
	for i, file := range files {
		shuffleSort.DoMapSequential(jobName, i, file, numberOfMapOutput, wordCountMapF, path, nil)
	}
	elapsed := time.Since(start)

	fmt.Println("Map phase took:", elapsed)

	start = time.Now()
	for i := 0; i < numberOfMapOutput; i++ {
		shuffleSort.DoReduceSequential(jobName, i, len(files), wordCountReduceF, path)
	}
	elapsed = time.Since(start)

	fmt.Println("Reduce phase took:", elapsed)

}

func wordCountDistributed(jobName string, files []string, numberOfMapOutput int, path string) {
	start := time.Now()
	shuffleSort.DoMapDistributed(jobName, files, numberOfMapOutput, wordCountMapF, path, nil)
	elapsed := time.Since(start)

	fmt.Println("Map phase took:", elapsed)

	start = time.Now()
	shuffleSort.DoReduceDistributed(jobName, numberOfMapOutput, len(files), wordCountReduceF, path)
	elapsed = time.Since(start)

	fmt.Println("Reduce phase took:", elapsed)

}
