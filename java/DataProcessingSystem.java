import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

// This class represents a single unit of ride-sharing or data-processing work
class Task {
    private final int taskId;
    private final String payload;

    public Task(int taskId, String payload) {
        this.taskId = taskId;
        this.payload = payload;
    }

    public int getTaskId() {
        return taskId;
    }

    public String getPayload() {
        return payload;
    }
}

// This class implements a thread-safe shared queue using ReentrantLock to avoid race conditions
class SharedTaskQueue {
    private final Queue<Task> queue = new LinkedList<>();
    private final ReentrantLock lock = new ReentrantLock();

    // This method safely adds a task to the shared queue while holding the lock
    public void addTask(Task task) {
        lock.lock();
        try {
            queue.add(task);
        } finally {
            // Always unlock inside finally so a failure never causes a deadlock
            lock.unlock();
        }
    }

    // This method safely retrieves and removes the next task, or returns null when empty
    public Task getTask() {
        lock.lock();
        try {
            return queue.poll();
        } finally {
            lock.unlock();
        }
    }
}

// This class defines the worker logic that each thread in the pool executes in parallel
class Worker implements Runnable {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    private final int workerId;
    private final SharedTaskQueue taskQueue;
    private final String outputFileName;
    private static final ReentrantLock fileLock = new ReentrantLock();

    public Worker(int workerId, SharedTaskQueue taskQueue, String outputFileName) {
        this.workerId = workerId;
        this.taskQueue = taskQueue;
        this.outputFileName = outputFileName;
    }

    @Override
    public void run() {
        logger.info("Worker-" + workerId + " started.");

        while (true) {
            // Retrieve the next available task safely from the shared queue
            Task task = taskQueue.getTask();

            // Exit the loop cleanly when all tasks in the queue have been processed
            if (task == null) {
                break;
            }

            try {
                // Simulate computational work delay (e.g., matching a ride or calculating fare)
                Thread.sleep(200);
                String processedResult = "Worker-" + workerId + " processed Task-" 
                                         + task.getTaskId() + " [" + task.getPayload().toUpperCase() + "]";

                // Write the processed result safely to the shared output file
                writeResultToFile(processedResult);

            } catch (InterruptedException e) {
                // Log the thread interruption and restore the interrupted state
                logger.log(Level.SEVERE, "Worker-" + workerId + " was interrupted.", e);
                Thread.currentThread().interrupt();
                break;
            }
        }

        logger.info("Worker-" + workerId + " completed all available tasks and is terminating.");
    }

    // This method synchronizes file writes so multiple threads do not overwrite or corrupt data
    private void writeResultToFile(String result) {
        fileLock.lock();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName, true))) {
            writer.write(result);
            writer.newLine();
            logger.info("Saved result: " + result);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Worker-" + workerId + " failed to write to file: " + e.getMessage(), e);
        } finally {
            fileLock.unlock();
        }
    }
}

public class DataProcessingSystem {
    private static final Logger logger = Logger.getLogger(DataProcessingSystem.class.getName());

    public static void main(String[] args) {
        SharedTaskQueue sharedQueue = new SharedTaskQueue();
        String outputFile = "java_output.txt";
        int totalTasks = 15;
        int numWorkers = 4;

        // Populate the shared queue with 15 ride-sharing data tasks
        for (int i = 1; i <= totalTasks; i++) {
            sharedQueue.addTask(new Task(i, "RideRequest_User_" + i));
        }
        logger.info("Added " + totalTasks + " tasks to the shared queue.");

        // Create a fixed thread pool with 4 worker threads using ExecutorService
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);

        // Submit all 4 workers to start pulling tasks concurrently
        for (int i = 1; i <= numWorkers; i++) {
            executor.submit(new Worker(i, sharedQueue, outputFile));
        }

        // Stop accepting new tasks and initiate an orderly shutdown
        executor.shutdown();

        try {
            // Wait up to 30 seconds for all worker threads to finish safely
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warning("Timeout reached before all workers finished; forcing shutdown.");
                executor.shutdownNow();
            } else {
                logger.info("All Java worker threads terminated safely.");
            }
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Main thread interrupted while awaiting termination.", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
