package invertedIndex

import (
	"fmt"
	"io/ioutil"
	"map-reduce/common"
	"map-reduce/shuffleSort"
	"strconv"
	"strings"
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
	common.MergeAlphabeticalOrder(numberOfMapOutput, jobName)
	iiTest(jobName, len(files))

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

func iiTest(jobName string, numberOfFiles int) {
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
