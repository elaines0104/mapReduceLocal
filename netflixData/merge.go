package netflixdata

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"map-reduce/common"
	"os"
	"sort"
	"strconv"
)

func merge(numberOfMapOutput int, jobName string) {
	fmt.Println("Merge phase")
	kvs := make(map[string]int)
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
			kvs[kv.Key], _ = strconv.Atoi(kv.Value)
		}
		file.Close()
	}
	n := map[int][]string{}
	var a []int
	for k, v := range kvs {
		n[v] = append(n[v], k)
	}
	for k := range n {
		a = append(a, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(a)))

	file, err := os.Create(common.ResultName(jobName))

	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range a {
		for _, s := range n[k] {
			fmt.Fprintf(w, "%s: %d\n", s, k)
		}
	}
	w.Flush()
	file.Close()
}
