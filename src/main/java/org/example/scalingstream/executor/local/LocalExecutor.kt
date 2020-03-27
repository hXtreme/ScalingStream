package org.example.scalingstream.executor.local

import de.jupf.staticlog.Log
import org.example.scalingstream.dag.Operator
import org.example.scalingstream.executor.*
import org.example.scalingstream.operator.Task
import org.example.scalingstream.stream.ChannelManager
import org.jgrapht.graph.DirectedAcyclicGraph

class LocalExecutor : Executor {

    override val type: String = "LOCAL"

    private val runningTasks: MutableSet<Pair<Operator<*, *, *, *>, Deployment>> = HashSet()

    override fun exec(dag: DirectedAcyclicGraph<Operator<*, *, *, *>, ChannelManager<*>>) {

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

    override val deploy: DeployFn = fun(operator: Operator<*, *, *, *>, task: () -> Task<*, *, *, *>): Deployment {
        val deployment = LocalDeployment(task)
        runningTasks.add(Pair(operator, deployment))
        return deployment
    }
}
