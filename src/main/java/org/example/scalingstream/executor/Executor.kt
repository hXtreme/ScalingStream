package org.example.scalingstream.executor

import org.example.scalingstream.dag.Operator
import org.example.scalingstream.operator.Task
import org.example.scalingstream.stream.ChannelManager
import org.jgrapht.graph.DirectedAcyclicGraph

typealias DeployFn = (Operator<*, *, *, *>, () -> Task<*, *, *, *>) -> Deployment

interface Executor {
    val type: String
    val name: String
        get() = "${type}_EXECUTOR"

    /**
     * Execute the Stream.
     * @param dag StreamExecutionDAG
     */
    fun exec(dag: DirectedAcyclicGraph<Operator<*, *, *, *>, ChannelManager<*>>): Unit

    val deploy: DeployFn
}
