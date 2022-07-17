package wordCount

import (
	"fmt"
	"io/ioutil"
	"map-reduce/common"
	shuffleSort "map-reduce/shuffleSort"
	"strconv"
	"strings"
	"time"
)

func WordCount(useCase string, jobName string, numberOfMapOutput int, path string, column []string) {
	files := common.OpenFiles(nil)

	jobName = jobName + "-WordCount"
	if useCase == "sequential" {
		wordCountSequential(jobName, files, numberOfMapOutput, path)
	} else if useCase == "distributed" {
		wordCountDistributed(jobName, files, numberOfMapOutput, path)
	}
	//common.Merge0rderByOccurrence(numberOfMapOutput, jobName)

	common.MergeAlphabeticalOrder(numberOfMapOutput, jobName)
	wordCountTest(jobName, len(files))

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

func wordCountTest(jobName string, numberOfFiles int) {
	resultName := strings.Split(jobName, "-")
	resultName = resultName[1:]

	resultFileName := "result-" + strconv.Itoa(numberOfFiles) + "files-" + strings.Join(resultName, "-") + ".txt"

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
