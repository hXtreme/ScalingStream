package page.parekh.scalingstream.executor

import page.parekh.scalingstream.dag.Operator
import page.parekh.scalingstream.operator.Task
import page.parekh.scalingstream.stream.StreamExecutionDAG

typealias DeployFn = (Operator<*, *, *, *>, () -> Task<*, *, *, *>) -> Deployment

interface Executor {
    val type: String
    val name: String
        get() = "${type}_EXECUTOR"

    /**
     * Execute the Stream.
     * @param dag StreamExecutionDAG
     */
    fun exec(dag: StreamExecutionDAG)

    val deploy: DeployFn
}
