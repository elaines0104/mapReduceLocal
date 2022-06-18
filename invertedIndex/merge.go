package invertedIndex

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"map-reduce/common"
	"os"
	"sort"
)

func merge(numberOfMapOutput int, jobName string) {
	//fmt.Println("Merge phase")
	kvs := make(map[string]string)
	for i := 0; i < numberOfMapOutput; i++ {
		p := common.ReduceOutputName(jobName, i)

		file, err := os.Open(p)
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv common.KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create(common.ResultName(jobName))
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	file.Close()
}
