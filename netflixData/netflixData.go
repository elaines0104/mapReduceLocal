package netflixdata

import (
	"fmt"
	"map-reduce/common"
	shuffleSort "map-reduce/shuffleSort"
	"time"
)

func NetflixData(method string, jobName string, numberOfMapOutput int, path string, column *string) {
	jobName = jobName + "-netflixData"

	files := common.OpenFiles(column)
	if method == "sequential" {
		wordCountSequential(jobName, files, numberOfMapOutput, path, column)
	} else if method == "distributed" {
		wordCountDistributed(jobName, files, numberOfMapOutput, path, column)
	}
	merge(numberOfMapOutput, jobName)
}
func wordCountSequential(jobName string, files []string, numberOfMapOutput int, path string, column *string) {
	start := time.Now()
	for i, file := range files {
		shuffleSort.DoMapSequential(jobName, i, file, numberOfMapOutput, netflixDataMapF, path, column)
	}
	elapsed := time.Since(start)

	fmt.Println("Map phase took:", elapsed)

	start = time.Now()
	for i := 0; i < numberOfMapOutput; i++ {
		shuffleSort.DoReduceSequential(jobName, i, len(files), netflixDataReduceF, path)
	}
	elapsed = time.Since(start)

	fmt.Println("Reduce phase took:", elapsed)

}

func wordCountDistributed(jobName string, files []string, numberOfMapOutput int, path string, column *string) {
	start := time.Now()
	shuffleSort.DoMapDistributed(jobName, files, numberOfMapOutput, netflixDataMapF, path, column)
	elapsed := time.Since(start)

	fmt.Println("Map phase took:", elapsed)

	start = time.Now()
	shuffleSort.DoReduceDistributed(jobName, numberOfMapOutput, len(files), netflixDataReduceF, path)

	elapsed = time.Since(start)

	fmt.Println("Reduce phase took:", elapsed)

}
