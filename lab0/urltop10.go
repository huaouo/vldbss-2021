package main

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
)

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	// YOUR CODE HERE :)
	// And don't forget to document your idea.
	var args RoundsArgs
	args = append(args, RoundArgs{
		MapFunc:    LocalMergeMap,
		ReduceFunc: LocalMergeReduce,
		NReduce:    nWorkers,
	})
	args = append(args, RoundArgs{
		MapFunc:    GlobalMergeMap,
		ReduceFunc: GlobalMergeReduce,
		NReduce:    1,
	})
	return args
}

type KCount struct {
	k     string
	count int64
}

func LocalMergeMap(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kCountMap := make(map[string]int64)
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		kCountMap[l]++
	}
	kvs := make([]KeyValue, 0, len(kCountMap))
	for k, count := range kCountMap {
		kvs = append(kvs, KeyValue{k, strconv.FormatInt(count, 10)})
	}
	return kvs
}

func LocalMergeReduce(key string, values []string) string {
	var keyCount int64
	for _, v := range values {
		if count, err := strconv.ParseInt(v, 10, 64); err != nil {
			log.Fatalln(err)
		} else {
			keyCount += count
		}
	}
	return fmt.Sprintf("%s %d\n", key, keyCount)
}

func GlobalMergeMap(filename string, contents string) []KeyValue {
	kvs := make([]KeyValue, 0)
	if contents == "" {
		return kvs
	}
	lines := strings.Split(strings.TrimSpace(contents), "\n")
	for _, l := range lines {
		kvs = append(kvs, KeyValue{"", l})
	}
	return kvs
}

func GlobalMergeReduce(key string, values []string) string {
	kCounts := make([]KCount, 0, len(values))
	for _, v := range values {
		kv := strings.Split(v, " ")
		if count, err := strconv.ParseInt(kv[1], 10, 64); err != nil {
			log.Fatalln(err)
		} else {
			kCounts = append(kCounts, KCount{kv[0], count})
		}
	}
	sort.Slice(kCounts, func(i, j int) bool {
		if kCounts[i].count != kCounts[j].count {
			return kCounts[i].count > kCounts[j].count
		}
		return kCounts[i].k < kCounts[j].k
	})
	var sb strings.Builder
	sliceLen := len(kCounts)
	if sliceLen > 10 {
		sliceLen = 10
	}
	for _, kcount := range kCounts[:sliceLen] {
		sb.WriteString(kcount.k)
		sb.WriteString(": ")
		sb.WriteString(strconv.FormatInt(kcount.count, 10))
		sb.WriteByte('\n')
	}
	result := sb.String()
	return result[:len(result)-1]
}
