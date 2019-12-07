package mapreduce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	//1. Read intermediate file
	//2. Sort according to the key
	//3. Call reduceF for each key
	//4. Write reduce output as JSON encoded keyValue - to file named - outFile

	keyValMap := make(map[string][]string)

	for m := 0; m < nMap; m++ {
		//find name of intermediate file
		intermediate_file_name := reduceName(jobName, m, reduceTask)
		//read json from intermediate_file_name
		//open file named intermediate_file_name
		fmt.Println("\nSAN: Reading file named: ", intermediate_file_name)
		file, err := ioutil.ReadFile(intermediate_file_name)
		if err != nil {
			fmt.Println("\nSAN: error reading file : ", intermediate_file_name)
			fmt.Println("\nERROR: ", err)
		}

		dec := json.NewDecoder(bytes.NewReader(file))

		for dec.More() {
			var m KeyValue
			dec.Decode(&m)
			fmt.Printf("\nKEY: %s VALUE: %s", m.Key, m.Value)
			//x["key"] = append(x["key"], "value")
			keyValMap[m.Key] = append(keyValMap[m.Key], m.Value)
			//keyValue_Array = append(keyValue_Array, m)
		}

		//sort
		/*sort.Slice(keyValue_Array[:], func(i, j int) bool {
			key1, _ := strconv.Atoi(keyValue_Array[i].Key)
			key2, _ := strconv.Atoi(keyValue_Array[j].Key)
			return key1 < key2
		})*/
	}
	//iterate
	var result_file, _ = os.OpenFile(outFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	enc := json.NewEncoder(result_file)

	/*for _, kv := range keyValue_Array {
		var val []string
		val = append(val, kv.Value)
		//fmt.Printf("\nSAN: Value of val: %v", val)
		reduceF_Result := reduceF(kv.Key, val) //TODO: reduce expects array of strings
		val = val[:0]
		enc.Encode(KeyValue{kv.Key, reduceF_Result})
	}*/
	for k, v := range keyValMap {
		reduceF_Result := reduceF(k, v)
		enc.Encode(KeyValue{k, reduceF_Result})
	}

	result_file.Close()

}
