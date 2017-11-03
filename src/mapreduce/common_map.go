package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!

	// Read file content
	content, err := ioutil.ReadFile(inFile)
	if err != nil {
		panic(err)
	}

	kvs := mapF(inFile, string(content))

	var rValues map[string][]KeyValue
	rValues = make(map[string][]KeyValue)
	for i := 0; i < nReduce; i++ {
		rValues[reduceName(jobName, mapTaskNumber, i)] = make([]KeyValue, 0)
	}

	// process KeyValue
	for _, kv := range kvs {
		rFile := reduceFileName(jobName, mapTaskNumber, nReduce, kv.Key)
		rValues[rFile] = append(rValues[rFile], kv)
	}

	// write to file
	for k, v := range rValues {
		f, err := os.Create(k)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		enc := json.NewEncoder(f)
		for _, kv := range v {
			err := enc.Encode(&kv)
			if err != nil {
				panic(err)
			}
		}
	}
}

func reduceFileName(jobName string, mapTaskNumber int, nReduce int, key string) string {
	h := ihash(key)
	nReduceTaskNumber := uint(h) % uint(nReduce)

	return reduceName(jobName, mapTaskNumber, int(nReduceTaskNumber))
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
