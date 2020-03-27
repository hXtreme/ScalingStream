package org.example.scalingstream.executor.local

import de.jupf.staticlog.Log
import org.example.scalingstream.dag.Operator
import org.example.scalingstream.executor.*
import org.example.scalingstream.operator.Task
import org.example.scalingstream.stream.ChannelManager
import org.jgrapht.graph.DirectedAcyclicGraph

class LocalExecutor : Executor {

    override val type: String = "LOCAL"

    private val runningTasks: MutableMap<Operator<*, *, *, *>, MutableList<Deployment>> = HashMap()

    override fun exec(dag: DirectedAcyclicGraph<Operator<*, *, *, *>, ChannelManager<*>>) {

        for (operators in dag) {
            operators.run(deploy)
        }

        runningTasks.forEach { (operator, deployments) ->
            while (deployments.isNotEmpty()) {
                deployments.forEach {
                    Log.debug("Joining back ${it.name}", name)
                    it.join()
                }
                operator.removeTasks(deployments.filter { it.isDone }.map { it.taskID })
                deployments.removeIf { it.isDone }
            }
        }
    }

    override val deploy: DeployFn = fun(operator: Operator<*, *, *, *>, task: () -> Task<*, *, *, *>): Deployment {
        val deployment = LocalDeployment(task)
        runningTasks.getOrPut(operator) { mutableListOf() }.add(deployment)
        return deployment
    }
}
