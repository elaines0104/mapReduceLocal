package netflixdata

import (
	"fmt"
	"io/ioutil"
	"map-reduce/common"
	shuffleSort "map-reduce/shuffleSort"
	"strings"
	"time"
)

func NetflixData(method string, jobName string, numberOfMapOutput int, path string, column *string) {
	jobName = jobName + "-netflixData-" + *column

	files := common.OpenFiles(column)
	if method == "sequential" {
		wordCountSequential(jobName, files, numberOfMapOutput, path, column)
	} else if method == "distributed" {
		wordCountDistributed(jobName, files, numberOfMapOutput, path, column)
	}
	//common.Merge0rderByOccurrence(numberOfMapOutput, jobName)
	common.MergeAlphabeticalOrder(numberOfMapOutput, jobName)
	netflixDataTest(jobName)
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
func netflixDataTest(jobName string) {
	resultName := strings.Split(jobName, "-")
	resultName = resultName[1:]

	resultFileName := "result-" + strings.Join(resultName, "-") + ".txt"

	resultFile, err := ioutil.ReadFile(resultFileName)
	if err != nil {
		fmt.Println(err)
	}

	jobFile, err := ioutil.ReadFile(common.ResultName(jobName))
	if err != nil {
		fmt.Println(err)
	}
	if string(resultFile) == string(jobFile) {
		fmt.Println("it worked")
	} else {
		fmt.Println("It did not work")
	}
}
