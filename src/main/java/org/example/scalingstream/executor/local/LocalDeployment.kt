package org.example.scalingstream.executor.local

import org.example.scalingstream.executor.AbstractDeployment
import org.example.scalingstream.operator.Task
import java.util.*

class LocalDeployment<InputType, FnInp, FnOut, OutputType>(
    val task: Task<InputType, FnInp, FnOut, OutputType>
) : AbstractDeployment<InputType, FnInp, FnOut, OutputType>(task) {
    private val thread = Thread(task, name)

    override var isDone: Boolean = false
        private set

    init {
        thread.start()
    }

    override fun join(){
        thread.join()
        isDone = true
    }

    override val taskID: UUID = task.taskID

    override val operatorID: String = task.operatorID

    override val numConsumed: Int
        get() = task.numConsumed

    override val numProduced: Int
        get() = task.numConsumed
}