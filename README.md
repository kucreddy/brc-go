# an attempt at brc, in go
***Desc***: read 1B rows from a single txt file (line format "city_name;temperature\n" -> temperature being float with 1 decimal precision), calculate the  min, mean, max of temperatures for each city

### Machine used - Base m2 pro macbook pro
- 10 cores (6 performance, 4 efficiency)
- Memory 16GB
- Storage bandwidth ~200GBps

### Base attempt
- single (main) goroutine
- simple scanner readlines

- Runtime - **4m 47.7sec**

### Variation 2 - add some parallelism
- split the file (abstract, not  modifying the actual file) into partitions, each worker goroutine processing one partition
- Since we are dealing with address in storage directly, we will have to parse the data explicitly
- each goroutine read the data 1 chunk at a time (chunk sized listed below), parsing the whole partition sequentially.
- each goroutine calculates the results for it's partition, returns the results in ot the results channel
- the main goroutine collest the results from each partition, and merges them (alas.... mapreduce!)
- ***we are using strings package to split the lines into city name, temperature; then converting the temperature string to float32*** 




| Goroutines    | ChunkSize | Runtime (sec) |
|---------------|-----------|---------------|
| 8             | 1MB       | 16.07         |
| 8             | 2MB       | 15.7          |
| 8             | 4MB       | 15.23         |
| 8             | 8MB       | 15.53         |
| 16            | 8MB       | 13.78         |

Increasing goroutines or chunksize further gave very inconsistent  results, so this is where we look elsewhere. We're freezing 16 goroutines and 8MB chunk size for now. 

### Variation 3 - parsing optimisation, and avoid flops
- start parsing the partition data byte by byte, do not rely on strings package (to avoid copying data, and to optimise the temperature processing)
- parse the bytes manually, convert the city name bytes to string without copying data (using unsafe package)
- avoid reading the temperatures as float32. the precision is exactly 1 decimal, so discard the '.' character, and read the whole thing as an integer. manually handling the negative signs (this is to keep the temperature operations like comparisons and summations as fast as reasonably possible)
- ***Runtime - 3.9-4.3 sec*** 😎 

Given that we have 200GBps disk bandwidth, we can probably go faster. 
So I took a look at the almightly cpu flame graph (this is where you need to pray for my mental health)

- runtime.mapaccess2_faststr ~44% (map lookups withing the goroutine, no direct global map access)
- readNextChunk (function that is doing  the IO) ~8.5%
- processPartition (this calls the above two, and  does byte  parsing) ~92%
- the other 7% were all schedulers, malloc etc. (I'll deal with these eventually)

1. After days of  fighting it, I gave up on maps. Since we are reading fresh slice of bytes for every row, map caching for the same string doesn't work, and it falls back to calculating new hash every time, irrespective of repeating names) . not much I can do here using golang map implementation. Try swiss tables and other data structures (tries are not so feasible  here as we are dealing with non ascii characters (european names 🤦), and preprocessing is cheating)
2. Processing is taking significantly longer than loading a chunk - maybe load more data chunks while the partition processing goroutine processes a chunk? this way the partition goroutine will never have to wait for any I/O, a potential 8.5% improvement in runtime (so naive, as we will see 😢)
3. But how can processing be slower than disk I/O, especially for such basic operations?!! Turns out (and I didn't know this earlier) CPU time doens't include I/O wait time, as the scheduler never picks the function up for execution until I/O signal is received. So technically it didn't get any CPU time despite taking a lot of runtime. Measuring this manually, each partition goroutine spent 1.2 - 1.5sec just waiting for I/O. So, possibly a potential 25-37% improvement 🤩 (again, so naive 😢, as we will see)

### Variation 4 - split each partition into multiple sub-partition, further parallelising IO
- spin up new goroutines doing only I/O, each working on a subpartition.
- Experimented with different variations of partitionCount (4-16), subPartitionCount (2-8), chunk size (2MB-16MB), and channel size (10-30)
- ***Runtime - 3.9-4.6 sec for all combinations - we somehow ended up worse*** 

Kill Me Now 😭😭

<WIP - I'll keep updating this doc>