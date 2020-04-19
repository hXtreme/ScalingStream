package page.parekh.scalingstream.executor.local

import page.parekh.scalingstream.executor.AbstractDeployment
import page.parekh.scalingstream.operator.Task
import java.util.*

class LocalDeployment(
    createTask: () -> Task<*, *, *, *>
) : AbstractDeployment() {
    private val task: Task<*, *, *, *> = createTask()
    private val thread = Thread(task, name)

    override val isRunning: Boolean
        get() = task.isRunning

    override val isDone: Boolean
        get() = task.isDone

    init {
        thread.start()
    }

    override fun join() {
        thread.join()
    }

    override val taskID: UUID
        get() {
            return task.taskID
        }

    override val operatorID: String
        get() {
            return task.operatorID
        }

    override val numConsumed: Int
        get() {
            return task.numConsumed
        }

    override val numProduced: Int
        get() {
            return task.numConsumed
        }
}
