package main

import (
	"fmt"
	"map-reduce/invertedIndex"
	netflixdata "map-reduce/netflixData"
	"map-reduce/wordCount"
	"os"
)

func main() {
	if len(os.Args) < 3 || (os.Args[1] == "netflix" && len(os.Args) < 3) {
		fmt.Println("Please read README.MD to see usage", os.Args[0])
		return
	}

	var numberOfMapOutput int
	var jobName string
	numberOfMapOutput = 8
	jobName = "teste"
	var path string = "/path/to/mapReduceLocal/"

	if os.Args[1] == "wordcount" {
		wordCount.WordCount(os.Args[2], jobName, numberOfMapOutput, path, nil)
	} else if os.Args[1] == "ii" {
		invertedIndex.Ii(os.Args[2], jobName, numberOfMapOutput, path, nil)
	} else if os.Args[1] == "netflix" {
		netflixdata.NetflixData(os.Args[2], jobName, numberOfMapOutput, path, &os.Args[3])

	}

}
