package page.parekh.scalingstream.taskrpc

import de.jupf.staticlog.Log
import org.apache.thrift.server.*
import org.apache.thrift.transport.TServerSocket
import page.parekh.scalingstream.executor.rpc.TaskRPCController
import page.parekh.scalingstream.operator.Task

class TaskServer(
    private val task: Task<*, *, *, *>,
    private val args: Map<String, Any>
) : Runnable {
    private val taskController = TaskRPCController(task)
    private val taskProcessor = TaskRPC.Processor(taskController)

    override fun run() {
        val port: Int = args.getOrDefault("port", 9090) as Int
        val socket = TServerSocket(port)
        val server = TSimpleServer(TServer.Args(socket).processor(taskProcessor))

        Log.info("Starting TaskServer(${task.name}) at port $port.")
        server.serve()
    }
}
