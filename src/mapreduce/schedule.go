package mapreduce

import "fmt"
import "sync"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	var wg sync.WaitGroup
	wg.Add(ntasks)

	for i := 0; i < ntasks; i++ {
		go func(i int) {
			for {
				worker := <-mr.registerChannel

				taskArgs := DoTaskArgs{
					JobName:       mr.jobName,
					File:          mr.files[i],
					Phase:         phase,
					TaskNumber:    i,
					NumOtherPhase: nios}
				var reply = new(struct{})
				ok := call(worker, "Worker.DoTask", taskArgs, reply)

				go func() {
					mr.registerChannel <- worker
				}()

				if ok {
					wg.Done()
					break
				} else {
					fmt.Printf("task %v done, fail", i)
				}

			}
		}(i)
	}

	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
