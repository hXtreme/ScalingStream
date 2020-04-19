package page.parekh.scalingstream.executor

import page.parekh.scalingstream.operator.Task
import java.time.Instant


data class TaskState(
    val timestamp: Instant,
    val isRunning: Boolean,
    val isDone: Boolean,
    val numConsumed: Int,
    val numProduced: Int
)

interface Deployment : Task<Any, Any, Any, Any> {

    fun state(): TaskState {
        return TaskState(Instant.now(), isRunning, isDone, numConsumed, numProduced)
    }

    /**
     * Waits for the corresponding task to die.
     */
    fun join()
}
