package org.example.scalingstream.executor.local

import org.example.scalingstream.executor.AbstractDeployment
import org.example.scalingstream.operator.Task
import java.util.*

class LocalDeployment(
    createTask: () -> Task<*, *, *, *>
) : AbstractDeployment(createTask) {
    private val task: Task<*, *, *, *> = createTask()
    private val thread: Thread

    override var isDone: Boolean = false
        private set

    init {
        thread = Thread(task, name)
        thread.start()
    }

    override fun join() {
        thread.join()
        isDone = true
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