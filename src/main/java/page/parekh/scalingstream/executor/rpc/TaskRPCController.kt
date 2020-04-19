package page.parekh.scalingstream.executor.rpc

import page.parekh.scalingstream.extensions.serializeToByteBuffer
import page.parekh.scalingstream.operator.Task
import page.parekh.scalingstream.taskrpc.TaskRPC
import page.parekh.scalingstream.taskrpc.TaskState
import java.nio.ByteBuffer
import java.time.Instant

class TaskRPCController(
    private val task: Task<*, *, *, *>
) : TaskRPC.Iface {
    private val taskID = task.taskID.serializeToByteBuffer()
    private val operatorID = task.operatorID
    private val thread = Thread(task, task.name)

    override fun taskID(): ByteBuffer {
        return taskID
    }

    override fun operatorID(): String {
        return operatorID
    }

    override fun isRunning(): Boolean = task.isRunning

    override fun isDone(): Boolean = task.isDone

    override fun numConsumed(): Int = task.numConsumed

    override fun numProduced(): Int = task.numProduced

    override fun getStatus(): TaskState {
        val timestamp = Instant.now().serializeToByteBuffer()
        return TaskState(timestamp, isRunning, isDone, task.numConsumed, task.numProduced)
    }

    override fun ping() {}

    override fun kill() {
        // TODO("Add task.kill")
        // task.kill()
        thread.join()
    }

    override fun run() {
        if (!(task.isDone || task.isRunning)) {
            thread.start()
        }
    }
}