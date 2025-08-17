package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type CityStats struct {
	Name  string
	Min   float32
	Mean  float32
	Max   float32
	Sum   float32
	Count int
}

var filePath = "../sample-10M.txt"
var processors = 4

func main() {
	resultChannels := make([]chan map[string]*CityStats, processors)
	counterChannels := make([]chan int, processors)
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
	partitionSize := fileStat.Size() / int64(processors)
	fmt.Printf("Partition size: %d bytes\n", partitionSize)

	nextStart := int64(0)
	currChar := make([]byte, 1)
	// waitGroup := sync.WaitGroup{}
	for i := 0; i < processors; i++ {
		start := nextStart
		end := start + partitionSize
		if i == processors-1 {
			end = fileStat.Size()
		} else {
			currChar = make([]byte, 1)
			for {
				_, err := file.ReadAt(currChar, end)
				if err != nil {
					fmt.Println("Error reading file:", err)
					return
				}
				if currChar[0] == '\n' {
					break
				}
				end++
			}
		}
		nextStart = end + 1
		resultChannels[i] = make(chan map[string]*CityStats)
		counterChannels[i] = make(chan int)
		// waitGroup.Add(1)
		go func(start, end int64, resultChannel chan map[string]*CityStats, counterChannel chan int) {
			// defer waitGroup.Done()

			fmt.Println("go routine started for range:", start, "to", end)

			// file, err := os.Open(FilePath)
			// if err != nil {
			// 	fmt.Println("Error opening file in goroutine:", err)
			// 	return
			// }
			// defer file.Close()

			res := make(map[string]*CityStats)
			counter := 0

			currStart := start
			for currStart < end {
				line, nextStart, err := readSingleLine(file, currStart)
				if err != nil {
					fmt.Println("Error reading line:", err)
					return
				}
				counter++
				currStart = nextStart
				parts := strings.Split(line, ";")
				cityName := parts[0]
				raw, err := strconv.ParseFloat(parts[1], 32)
				if err != nil {
					fmt.Println("Error parsing measurement:", err)
					return
				}
				measurement := float32(raw)
				if stats, exists := res[cityName]; exists {
					stats.Count++
					stats.Sum += measurement
					if measurement < stats.Min {
						stats.Min = measurement
					}
					if measurement > stats.Max {
						stats.Max = measurement
					}
				} else {
					res[cityName] = &CityStats{
						Name:  cityName,
						Min:   measurement,
						Mean:  measurement,
						Max:   measurement,
						Sum:   measurement,
						Count: 1,
					}
				}
			}

			resultChannel <- res
			counterChannel <- counter

		}(start, end, resultChannels[i], counterChannels[i])

	}
	// fmt.Println("All go routines started, waiting for them to finish...")
	// waitGroup.Wait()

	// merge the results into citiesMap
	for _, resultChannel := range resultChannels {
		result := <-resultChannel
		for cityName, stats := range result {
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
	}

	// print counters
	for i, counterChannel := range counterChannels {
		counter := <-counterChannel
		fmt.Printf("routine %d processed %d lines\n", i, counter)
		close(counterChannel)
	}

	resultsFile, err := os.Create("results.txt")
	if err != nil {
		fmt.Println("Error creating results file:", err)
		return
	}
	defer resultsFile.Close()
	for _, stats := range citiesMap {
		stats.Mean = stats.Sum / float32(stats.Count)
		_, err := fmt.Fprintf(resultsFile, "%-20s\t%.2f\t%.2f\t%.2f\t%d\n", stats.Name, stats.Min, stats.Mean, stats.Max, stats.Count)
		if err != nil {
			fmt.Println("Error writing to results file:", err)
			return
		}
	}
	fmt.Printf("Time taken: %s\n", time.Since(start))
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
