package page.parekh.scalingstream.executor.rpc

import de.jupf.staticlog.Log
import page.parekh.scalingstream.dag.Operator
import page.parekh.scalingstream.executor.DeployFn
import page.parekh.scalingstream.executor.Deployment
import page.parekh.scalingstream.executor.Executor
import page.parekh.scalingstream.stream.StreamExecutionDAG
import page.parekh.scalingstream.taskrpc.TaskServer
import kotlin.math.abs

class RPCExecutor : Executor {
    override val type: String = "THRIFT"

    private val runningTasks: MutableSet<Pair<Operator<*, *, *, *>, Deployment>> = HashSet()

    @Suppress("DuplicatedCode")
    override fun exec(dag: StreamExecutionDAG) {

        for (operators in dag) {
            operators.run(deploy)
        }

        while (runningTasks.isNotEmpty()) {
            runningTasks.forEach { (operator, deployment) ->
                Log.debug("Joining back ${deployment.name}", name)
                deployment.join()
                operator.removeTask(deployment.taskID)
            }
            runningTasks.removeIf{ (_, deployment) -> deployment.isDone }
        }
    }

    override val deploy: DeployFn = fun(operator, taskConstructor): Deployment {
        val task = taskConstructor()

        // TODO("Get rid of the magic constants.")
        val host: String = "localhost"
        val port: Int = abs(task.taskID.hashCode() % (PORT_MAX - PORT_OFFSET)) + (PORT_OFFSET + 1)


        // 1: Setup the TaskServer and run it.
        val server = TaskServer(task, mapOf(Pair("port", port)))
        with(Thread(server)) {
            this.isDaemon = true
            this.start()
        }

        // 2: Setup the TaskAdapter (Deployment)
        val deployment: Deployment = RPCDeployment(host, port)
        runningTasks.add(Pair(operator, deployment))

        // Return the TaskAdapter
        return deployment
    }

    private companion object {
        const val PORT_MAX = 65535
        const val PORT_OFFSET = 1024
    }
}