package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase).
// the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task.
// nReduce is the number of reduce tasks.
// the registerChan argument yields a stream of registered workers; each item is the worker's RPC address,
// suitable for passing to call().
// registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	switch phase {
	case mapPhase:
		fmt.Printf("\nSANMAP_PHASE - Number of map files : %d", len(mapFiles))
		var wg sync.WaitGroup

		for index, InputFile := range mapFiles {
			fmt.Printf("\nSAN: Processing: index: %d InputFile: %s", index, InputFile)
			x, ok := <-registerChan
			if ok {
				fmt.Printf("SAND-map: Worker %s was read.\n", x)
				method := "Worker.DoTask"
				s := DoTaskArgs{JobName: jobName, File: InputFile, Phase: phase, TaskNumber: index, NumOtherPhase: n_other}
				wg.Add(1)
				go func() {
					//defer fmt.Printf("\nSANEND-map: work done: index - %d\n", index)
					//var call_reply bool
					defer re_register(registerChan, x /*, call_reply*/)
					defer wg.Done()
					call_reply := call(x, method, s, nil)
					if call_reply == false {
						fmt.Printf("\nSAN-CALL-REPLY-FALSE-MAP")
					} else {
						fmt.Printf("\nSAN-CALL-REPLY-TRUE-MAP")
					}
					//defer re_register(registerChan, x /*, call_reply*/)
				}()
			} else {
				fmt.Println("Channel Closed")
			}
		}
		wg.Wait()

	case reducePhase:
		var wg sync.WaitGroup

		for i := 0; i < nReduce; i++ {
			fmt.Printf("\nSAN: Processing reduce index: %d", i)
			x, ok := <-registerChan
			if ok {
				fmt.Printf("SAND-reduce: Value %s was read.\n", x)
				method := "Worker.DoTask"
				s := DoTaskArgs{JobName: jobName, File: "", Phase: phase, TaskNumber: i, NumOtherPhase: n_other}
				wg.Add(1)
				go func() {
					var call_reply bool
					defer fmt.Printf("\nSANEND-reduce: work done\n")
					defer re_register(registerChan, x)
					defer wg.Done()
					call_reply = call(x, method, s, nil)
					if call_reply == false {
						fmt.Printf("\nSAN-CALL-REPLY-FALSE-REDUCE- keeping index: %d", i)
						i-- // currently worker is added back to the channel. Try not to do it.
					} else {
						fmt.Printf("\nSAN-CALL-REPLY-TRUE-REDUCE - index: %d Reregistering worker", i)
						//re_register(registerChan, x)
						//registerChan <- x
					}
				}()
			} else {
				fmt.Println("Channel Closed")
			}
		}
		fmt.Printf("\nSAN: Waiting - REDUCE")
		wg.Wait()
		fmt.Printf("\nSAN: Waiting - REDUCE - done")
	}

	fmt.Printf("Schedule: %v done\n", phase)

}

func re_register(ch chan string, worker string /*, call_reply bool*/) {
	fmt.Printf("\nSAN: FUNCTION: re_register Worker name added: %s", worker)
	ch <- worker
}
