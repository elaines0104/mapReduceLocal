package shuffleSort

import (
	"encoding/json"
	"fmt"
	"map-reduce/common"
	"os"
	"sort"
)

func DoMapSequential(
	jobName string,
	mapTaskNumber int,
	inFile string,
	numberOfMapOutput int,
	mapF func(file string, contents string) []common.KeyValue,
	path string,
	column *string) {

	kvList := mapF(inFile, getContent(inFile, column))

	for r := 0; r < numberOfMapOutput; r++ {
		reduceFileName := common.MapOutputName(jobName, mapTaskNumber, r)
		fullPath := path + reduceFileName

		reduceFile, err := os.Create(fullPath)
		if err != nil {
			fmt.Println(err)
		}
		enc := json.NewEncoder(reduceFile)
		for _, kv := range kvList {
			if (int(ihash(kv.Key)) % numberOfMapOutput) == r {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println(err)
				}

			}
		}
		reduceFile.Close()

	}
}

func DoReduceSequential(
	jobName string,
	reduceTaskNumber int,
	numberOfFiles int,
	reduceF func(key string, values []string) string,
	path string) {
	mapKeyValue := make(map[string][]string)

	for m := 0; m < numberOfFiles; m++ {

		fileName := common.MapOutputName(jobName, m, reduceTaskNumber)
		fullPath := path + fileName

		file, _ := os.Open(fullPath)
		dec := json.NewDecoder(file)
		for {
			var kv common.KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := mapKeyValue[kv.Key]
			if !ok {
				mapKeyValue[kv.Key] = []string{}
			}
			mapKeyValue[kv.Key] = append(mapKeyValue[kv.Key], kv.Value)
		}
		file.Close()
		var keys []string
		for k := range mapKeyValue {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		merged := common.ReduceOutputName(jobName, reduceTaskNumber)

		file, _ = os.Create(merged)
		enc := json.NewEncoder(file)
		for _, k := range keys {
			enc.Encode(common.KeyValue{Key: k, Value: reduceF(k, mapKeyValue[k])})
		}
		file.Close()

	}

}
