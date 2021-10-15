package main

import (
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/alibaba/polardbx-operator/third-party/hsperfdata"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Printf("Usage: hsstat pid\n")
		return
	}

	filePath := os.Args[1]
	// filePath, err := hsperfdata.PerfDataPath(pid)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	entryMap, err := hsperfdata.ReadPerfData(filePath, true)
	if err != nil {
		log.Fatal("open fail", err)
	}

	var keys []string
	for k := range entryMap {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, key := range keys {
		fmt.Printf("%s=%v\n", key, entryMap[key])
	}
}
