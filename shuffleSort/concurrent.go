package shuffleSort

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"map-reduce/common"
	"os"
	"sync"
)

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
func DoMapConcurrent(jobName string,
	files []string,
	numberOfMapOutput int,
	mapF func(file string, contents string) []common.KeyValue,
	path string,
	column *string) {
	var wg sync.WaitGroup
	for i, file := range files {
		wg.Add(1)
		file := file
		i := i
		go func() {
			defer wg.Done()
			doMapConcurrent(jobName, i, file, numberOfMapOutput, mapF, path, column)
		}()

	}
	wg.Wait()

}

func doMapConcurrent(
	jobName string,
	mapTaskNumber int,
	inFile string,
	numberOfMapOutput int,
	mapF func(file string, contents string) []common.KeyValue,
	path string,
	column *string) {

	var wg sync.WaitGroup

	kvList := mapF(inFile, getContent(inFile, column))

	for r := 0; r < numberOfMapOutput; r++ {

		wg.Add(1)

		r := r
		go func() {
			defer wg.Done()
			doMapConcurrentLoop(jobName, mapTaskNumber, numberOfMapOutput, kvList, r, path)
		}()

	}
	wg.Wait()

}
func doMapConcurrentLoop(jobName string, mapTaskNumber int, nReduce int, kvList []common.KeyValue, count int, path string) {
	reduceFileName := common.MapOutputName(jobName, mapTaskNumber, count)
	fullPath := path + reduceFileName

	reduceFile, err := os.Create(fullPath)
	if err != nil {
		fmt.Println(err)
	}
	enc := json.NewEncoder(reduceFile)
	for _, kv := range kvList {
		if (int(ihash(kv.Key)) % nReduce) == count {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println(err)
			}

		}
	}
	reduceFile.Close()

}
func DoReduceConcurrent(
	jobName string,
	numberOfMapOutput int,
	numberOfFiles int,
	reduceF func(key string, values []string) string,
	path string) {

	var wg sync.WaitGroup

	for m := 0; m < numberOfMapOutput; m++ {
		wg.Add(1)
		m := m

		go func() {
			defer wg.Done()

			DoReduceSequential(jobName, m, numberOfFiles, reduceF, path)

		}()

	}
	wg.Wait()
}
