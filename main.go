package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"
	"unsafe"
)

type CityStats struct {
	Name  string
	Min   int
	Mean  float32
	Max   int
	Sum   int
	Count int
}

var filePath = "../measurements-1000000000.txt"
var partitioinCount = 16
var subPartitionCount = 2
var chunkSize = 1 * 1024 * 1024 // size of each chunk to read in bytes

func main() {

	cpu_prof, err := os.Create("cpu.prof")
	if err != nil {
		fmt.Println("could not create CPU profile: ", err)
		return
	}
	defer cpu_prof.Close()
	if err = pprof.StartCPUProfile(cpu_prof); err != nil {
		fmt.Println("could not start CPU profile: ", err)
		return
	}
	defer pprof.StopCPUProfile()

	resultChannels := make([]chan *partitionResult, partitioinCount)
	start := time.Now()
	citiesMap := make(map[string]*CityStats)
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file size:", err)
		return
	}
	partitionSize := fileStat.Size() / int64(partitioinCount)
	// fmt.Printf("Partition size: %d bytes\n", partitionSize)
	fmt.Printf("Partition count: %d, Partition size: %d bytes\n", partitioinCount, partitionSize)

	nextStart := int64(0)
	currChar := make([]byte, 1)
	// waitGroup := sync.WaitGroup{}
	for i := 0; i < partitioinCount; i++ {
		start := nextStart
		end := start + partitionSize
		if i == partitioinCount-1 {
			end = fileStat.Size() - 1
		} else {
			currChar = make([]byte, 200) // any cityname should reasonably fit in this
			_, err := file.ReadAt(currChar, end)
			if err != nil {
				fmt.Println("Error reading file:", err)
				return
			}
			i := 0
			for {
				c := currChar[i]
				if c == '\n' {
					break
				}
				i++
			}
			end += int64(i)
		}
		nextStart = end + 1
		resultChannels[i] = make(chan *partitionResult)
		// waitGroup.Add(1)
		go processPartition(file, start, end, resultChannels[i])
	}

	totalIoWait := time.Duration(0)
	for _, resultChannel := range resultChannels {
		res := <-resultChannel
		data := res.data
		totalIoWait += res.ioWait
		for cityName, stats := range data {
			if existingStats, exists := citiesMap[cityName]; exists {
				existingStats.Count += stats.Count
				existingStats.Sum += stats.Sum
				if stats.Min < existingStats.Min {
					existingStats.Min = stats.Min
				}
				if stats.Max > existingStats.Max {
					existingStats.Max = stats.Max
				}
			} else {
				citiesMap[cityName] = stats
			}
		}
		close(resultChannel)
		// fmt.Printf("Processed %d lines in partition\n", res.count)
		fmt.Printf("Processed %d lines in partition, IO wait time: %s\n", res.count, res.ioWait)
	}
	// avgIoWait := totalIoWait / time.Duration(partitioinCount)
	// fmt.Printf("Avg IO wait time per partition: %s\n", avgIoWait)

	resultsFile, err := os.Create("results.txt")
	if err != nil {
		fmt.Println("Error creating results file:", err)
		return
	}
	defer resultsFile.Close()
	for _, stats := range citiesMap {
		stats.Mean = float32(stats.Sum) / float32(stats.Count)
		_, err := fmt.Fprintf(resultsFile, "%-20s\t%.1f\t%.1f\t%.1f\t%d\n", stats.Name, float64(stats.Min)/10, float64(stats.Mean)/10, float64(stats.Max)/10, stats.Count)
		if err != nil {
			fmt.Println("Error writing to results file:", err)
			return
		}
	}
	fmt.Printf("Time taken: %s\n", time.Since(start))
}

type partitionResult struct {
	data   map[string]*CityStats
	count  int
	ioWait time.Duration
}

func processPartition(file *os.File, start, end int64, resultChannel chan *partitionResult) {

	// fmt.Println("go routine started for range:", start, "to", end)

	res := make(map[string]*CityStats, 500)
	counter := 0

	dataChan := make(chan chunkResponse, 30)
	go loadPartitionDataInChunks(file, start, end, dataChan)

	var ioWait time.Duration

	finishedPartitions := 0

	for finishedPartitions < subPartitionCount {
		ioStart := time.Now()
		chunkResp := <-dataChan
		ioWait += time.Since(ioStart)
		if chunkResp.err != nil {
			fmt.Println("Error reading lines:", chunkResp.err)
			return
		}
		if len(chunkResp.data) == 0 {
			finishedPartitions++
		}
		i := 0
		j := 0
		data := chunkResp.data
		for i < len(data) {
			// cityNameBytes := make([]byte, 0)
			j = i
			for data[j] != ';' {
				j++
			}
			cityNameBytes := data[i:j]
			cityName := *(*string)(unsafe.Pointer(&cityNameBytes))

			i = j + 1
			_sign := 1
			if data[i] == '-' {
				_sign = -1
				i++
			}
			temp := 0
			for data[i] != '\n' { // try and make this loop cheaper.
				if data[i] != '.' {
					temp = temp*10 + int(data[i]-'0')
				}
				i++
			}
			i++
			temp *= _sign
			if stats, exists := res[cityName]; exists { // this is the bottleneck
				stats.Count++
				stats.Sum += temp
				if temp < stats.Min {
					stats.Min = temp
				}
				if temp > stats.Max {
					stats.Max = temp
				}
			} else {
				res[cityName] = &CityStats{
					Name:  cityName,
					Min:   temp,
					Max:   temp,
					Sum:   temp,
					Count: 1,
				}
			}
			counter++
		}
	}
	partitionRes := &partitionResult{
		data:   res,
		count:  counter,
		ioWait: ioWait,
	}
	resultChannel <- partitionRes
}

func readSingleLine(file *os.File, start int64) (line string, nextStart int64, err error) {
	bytes := make([]byte, 0, 100) // the input lines should reasonably be under 100 characters
	nextByte := make([]byte, 1)
	for {
		_, err := file.ReadAt(nextByte, start)
		if err != nil {
			break // should ideally never happen
		}
		if nextByte[0] == '\n' {
			break
		}
		bytes = append(bytes, nextByte[0])
		start++
	}
	line = string(bytes)
	nextStart = start + 1 // move to the next character after the newline
	return line, nextStart, nil
}

type chunkResponse struct {
	data      []byte
	nextStart int64
	err       error
}

func loadPartitionDataInChunks(file *os.File, start, end int64, resChan chan chunkResponse) {

	subPartitionSize := (end - start + 1) / int64(subPartitionCount)
	currStart := start
	for i := 0; i < subPartitionCount; i++ {
		subPartitionEnd := currStart + subPartitionSize - 1
		if i == subPartitionCount-1 {
			subPartitionEnd = end
		} else {
			currChar := make([]byte, 1)
			for {
				_, err := file.ReadAt(currChar, subPartitionEnd)
				if err != nil {
					fmt.Println("Error reading file:", err)
					return
				}
				if currChar[0] == '\n' {
					break
				}
				subPartitionEnd++
			}
		}
		go readSubPartitionChunks(file, currStart, subPartitionEnd, resChan)
		currStart = subPartitionEnd + 1
	}
}

func readSubPartitionChunks(file *os.File, currStart, subPartitionEnd int64, resChan chan chunkResponse) {
	// read chunk size bytes, and seek back to the last newline
	// if currStart + chunkSize > partitionEnd -> read only till partitionEnd

	if currStart >= subPartitionEnd {
		res := chunkResponse{
			data:      nil,
			nextStart: currStart,
			err:       nil,
		}
		resChan <- res
		return
	}

	res := chunkResponse{}

	bufferSize := chunkSize
	if currStart+int64(chunkSize) > subPartitionEnd {
		bufferSize = int(subPartitionEnd-currStart) + 1
	}
	bytes := make([]byte, bufferSize)
	readSize, err := file.ReadAt(bytes, currStart)
	if err != nil {
		fmt.Println("Error reading file:", err)
		res.err = err
		resChan <- res
		return
	}
	if readSize != int(bufferSize) {
		fmt.Printf("Warning: read size %d does not match expected size %d\n", readSize, bufferSize)
		res.err = fmt.Errorf("read size %d does not match expected size %d", readSize, bufferSize)
		resChan <- res
		return
	}

	// from the last byte, seek back to the last newline
	for i := readSize - 1; i >= 0; i-- {
		if bytes[i] == '\n' {
			res.nextStart = currStart + int64(i+1)
			bytes = bytes[:i+1] // trim the bytes to only include up to the last newline
			break
		}
		// for our dataset, we won't reach the start of the chunk without finding a newline
	}
	res.data = bytes
	resChan <- res

	go readSubPartitionChunks(file, res.nextStart, subPartitionEnd, resChan)
}
