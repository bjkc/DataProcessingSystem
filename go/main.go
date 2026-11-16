package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// Task represents a single unit of ride-sharing data to be processed
type Task struct {
	ID      int
	Payload string
}

// SharedQueue wraps a concurrency-safe buffered channel to distribute tasks
type SharedQueue struct {
	tasks chan Task
}

// NewSharedQueue creates a buffered channel with the given capacity
func NewSharedQueue(capacity int) *SharedQueue {
	return &SharedQueue{
		tasks: make(chan Task, capacity),
	}
}

// AddTask safely places a new task into the shared channel
func (sq *SharedQueue) AddTask(t Task) {
	sq.tasks <- t
}

// GetTask retrieves a task from the channel; ok becomes false when closed and empty
func (sq *SharedQueue) GetTask() (Task, bool) {
	t, ok := <-sq.tasks
	return t, ok
}

// Close closes the channel so worker goroutines know no more tasks are coming
func (sq *SharedQueue) Close() {
	close(sq.tasks)
}

// worker runs as a lightweight goroutine that pulls tasks and writes processed results
func worker(id int, queue *SharedQueue, file *os.File, fileMutex *sync.Mutex, wg *sync.WaitGroup) {
	// Guarantee the WaitGroup counter decrements when this goroutine finishes
	defer wg.Done()

	log.Printf("Worker-%d started.\n", id)

	for {
		// Safely pull the next task from the channel
		task, ok := queue.GetTask()
		if !ok {
			// Exit the loop cleanly once the channel is closed and drained
			break
		}

		// Simulate computational delay (e.g., route matching and fare calculation)
		time.Sleep(200 * time.Millisecond)

		result := fmt.Sprintf("Worker-%d processed Task-%d [%s]\n", id, task.ID, strings.ToUpper(task.Payload))

		// Lock the mutex before writing to the shared output file to prevent race conditions
		fileMutex.Lock()
		_, err := file.WriteString(result)
		fileMutex.Unlock()

		// Explicit Go error handling after file write operation
		if err != nil {
			log.Printf("ERROR: Worker-%d failed to write task %d to file: %v\n", id, task.ID, err)
			continue
		}

		log.Printf("Saved result: %s", strings.TrimSpace(result))
	}

	log.Printf("Worker-%d completed all tasks and is terminating.\n", id)
}

func main() {
	totalTasks := 15
	numWorkers := 4
	outputFileName := "go_output.txt"

	// Open or create the shared output file and check for errors immediately
	file, err := os.OpenFile(outputFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("CRITICAL: Failed to open output file: %v\n", err)
	}
	// Use defer to ensure the file resource is always closed cleanly when main exits
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("ERROR: Failed to close output file: %v\n", closeErr)
		}
	}()

	// Create the shared queue and load 15 tasks into it
	queue := NewSharedQueue(totalTasks)
	for i := 1; i <= totalTasks; i++ {
		queue.AddTask(Task{
			ID:      i,
			Payload: fmt.Sprintf("RideRequest_User_%d", i),
		})
	}
	// Close the channel after populating so workers terminate gracefully once finished
	queue.Close()
	log.Printf("Added %d tasks to the shared queue.\n", totalTasks)

	var wg sync.WaitGroup
	var fileMutex sync.Mutex

	// Start 4 concurrent worker goroutines
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, queue, file, &fileMutex, &wg)
	}

	// Wait until all worker goroutines call wg.Done()
	wg.Wait()
	log.Println("All Go worker goroutines terminated safely.")
}
