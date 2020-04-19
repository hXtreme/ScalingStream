package org.example.scalingstream.stream

import org.example.scalingstream.dag.Operator
import java.time.Instant

class StreamMonitor(val streamDAG: StreamExecutionDAG) {
    data class TaskStat()
    data class OperatorStat(val name: String, val parallelism: Int, val taskStats: List<TaskStat>)
    data class StreamStat(val timestamp: Instant)

    private var lastChecked: Instant = Instant.now()

    fun updateStats() {
        val timestamp = Instant.now()
        val operatorStatsList = streamDAG.map { operator -> getStats(operator) }
    }

    private fun getStats(operator: Operator<*, *, *, *>) {
        val name = operator.name
        val parallelism = operator.parallelism
        val (numConsumed, numProduced) = operator.deployedTasks.map { (k, v) -> Pair(Pair(k, v.numConsumed), Pair(k, v.numProduced))}.unzip()
    }
}