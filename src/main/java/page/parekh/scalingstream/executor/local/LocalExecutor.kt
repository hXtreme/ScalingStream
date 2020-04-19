package page.parekh.scalingstream.executor.local

import de.jupf.staticlog.Log
import page.parekh.scalingstream.dag.Operator
import page.parekh.scalingstream.executor.*
import page.parekh.scalingstream.operator.Task
import page.parekh.scalingstream.stream.StreamExecutionDAG

class LocalExecutor : Executor {

    override val type: String = "LOCAL"

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

    override val deploy: DeployFn = fun(operator: Operator<*, *, *, *>, task: () -> Task<*, *, *, *>): Deployment {
        val deployment = LocalDeployment(task)
        runningTasks.add(Pair(operator, deployment))
        return deployment
    }
}
