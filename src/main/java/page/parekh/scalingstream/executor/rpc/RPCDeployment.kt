package page.parekh.scalingstream.executor.rpc

import de.jupf.staticlog.Log
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.*
import page.parekh.scalingstream.executor.*
import page.parekh.scalingstream.extensions.deserializeFromByteBuffer
import page.parekh.scalingstream.taskrpc.TaskRPC
import java.util.*

class RPCDeployment(
    private val host: String,
    private val port: Int
) : AbstractDeployment() {
    private val taskClient: TaskRPC.Client
    private val transport = TSocket(host, port)

    override val name = "Deployment @ ($host:$port)"

    init {
        Log.debug("Attempting to connect to $name")
        while (!transport.isOpen) {
            try {
                transport.open()
            } catch (e: TTransportException) {
                Log.warn("Failed to connect to deployed task, retrying...", name)
                Thread.sleep(100)
            }
        }
        Log.debug("Successfully connected to $name")
        val protocol = TBinaryProtocol(transport)
        taskClient = TaskRPC.Client(protocol)
        taskClient.run()
    }

    override val taskID: UUID = deserializeFromByteBuffer(taskClient.taskID())

    override val operatorID: String = taskClient.operatorID()

    override val isRunning: Boolean
        get() = taskClient.isRunning

    override val isDone: Boolean
        get() = taskClient.isDone

    override val numConsumed: Int
        get() = taskClient.numConsumed()

    override val numProduced: Int
        get() = taskClient.numProduced()

    override fun state(): TaskState {
        val rpcStatus = taskClient.status
        return TaskState(
            deserializeFromByteBuffer(rpcStatus.timestamp),
            rpcStatus.isRunning,
            rpcStatus.isDone,
            rpcStatus.numConsumed,
            rpcStatus.numProduced
        )
    }

    override fun join() {
        while (!taskClient.isDone) {
            Thread.sleep(100)
        }
    }
}