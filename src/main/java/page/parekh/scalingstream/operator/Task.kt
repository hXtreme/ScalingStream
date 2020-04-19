package page.parekh.scalingstream.operator

import java.util.*

/**
 * A Task that [run] an instance of a particular [Operator][org.example.scalingstream.dag.Operator]
 */
interface Task<InputType, FnInp, FnOut, OutputType> : Runnable {
    val taskID: UUID
    val operatorID: String

    /**
     * Human-friendly name for this [Task]
     */
    val name: String
        get() {
            return "$operatorID-${taskID.toString().substring(0, 5)}"
        }

    /**
     * The number of records consumed by this task.
     */
    val numConsumed: Int

    /**
     * The number of records produced by this task.
     */
    val numProduced: Int

    /**
     * Is this Task done running?
     */
    val isDone: Boolean

    /**
     * Is this Task running?
     */
    val isRunning: Boolean

    /**
     * Run the [Task]
     * @see Runnable.run()
     */
    override fun run()
}
