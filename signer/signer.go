package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type hashResult struct {
	number int
	hash   string
}

const COUNTER = 6

func ExecutePipeline(hashSignJobs ...job) {
	wg := &sync.WaitGroup{}
	input := make(chan interface{})
	for _, jobItem := range hashSignJobs {
		wg.Add(1)
		output := make(chan interface{})
		go func(jobFunc job, in chan interface{}, out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			defer close(out)
			jobFunc(in, out)
		}(jobItem, input, output, wg)
		input = output
	}
	defer wg.Wait()
}

func SingleHash(in chan interface{}, out chan interface{}) {
	wg := &sync.WaitGroup{}
	hashChan := make(chan string)
	for data := range in {
		wg.Add(1)
		data := fmt.Sprintf("%v", data)
		md5 := DataSignerMd5(data)
		go func(data string, hashMd5 string) {
			if data == "8" {
				defer close(hashChan)
			}
			defer wg.Done()
			crt32 := getCrt32Data(data)
			r32 := DataSignerCrc32(hashMd5)
			l32 := <-crt32
			hashChan <- l32 + "~" + r32
		}(data, md5)
	}
	for hashResult := range hashChan {
		out <- hashResult
	}
	defer wg.Wait()
}

func MultiHash(in chan interface{}, out chan interface{}) {
	wg := &sync.WaitGroup{}
	output := make(chan string)
	for input := range in {
		wg.Add(1)
		wgTemp := &sync.WaitGroup{}
		data := input.(string)
		inputChan := make(chan hashResult)
		wgTemp.Add(COUNTER)
		for i := 0; i < COUNTER; i++ {
			go GetMultiHashProcess(inputChan, wgTemp, data, i)
		}
		go func(wg1 *sync.WaitGroup, c chan hashResult) {
			defer close(c)
			wg1.Wait()
		}(wgTemp, inputChan)
		go func(resHash chan hashResult, output chan string, wg *sync.WaitGroup) {
			defer wg.Done()
			result := map[int]string{}
			var data []int
			for hashResult := range resHash {
				result[hashResult.number] = hashResult.hash
				data = append(data, hashResult.number)
			}
			sort.Ints(data)
			var resSet []string
			for i := range data {
				resSet = append(resSet, result[i])
			}
			output <- strings.Join(resSet, "")
		}(inputChan, output, wg)
	}

	go func(wgOut *sync.WaitGroup, c chan string) {
		defer close(c)
		wgOut.Wait()
	}(wg, output)

	for hash := range output {
		out <- hash
	}

}

func GetMultiHashProcess(hashResultChan chan hashResult, wg *sync.WaitGroup, singleHash interface{}, i int) {
	defer wg.Done()
	hashResultChan <- hashResult{number: i, hash: DataSignerCrc32(fmt.Sprintf("%v%v", i, singleHash))}
}

func getCrt32Data(data string) chan string {
	res := make(chan string, 1)
	go func(output chan<- string) {
		output <- DataSignerCrc32(data)
	}(res)
	return res
}

func CombineResults(in, out chan interface{}) {
	var res string
	var resHash []string
	for hashResult := range in {
		resHash = append(resHash, (hashResult).(string))
	}
	sort.Strings(resHash)
	res = strings.Join(resHash, "_")
	out <- res
}
