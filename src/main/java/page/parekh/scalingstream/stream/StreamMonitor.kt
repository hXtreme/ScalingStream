package page.parekh.scalingstream.stream

import com.google.gson.Gson
import de.jupf.staticlog.Log
import page.parekh.scalingstream.executor.TaskState
import java.time.Instant
import java.util.*

class StreamMonitor(val streamDAG: StreamExecutionDAG) : Runnable {
    val gson = Gson()

    data class OperatorStat(
        val name: String,
        val parallelism: Int,
        val taskStats: List<Pair<UUID, TaskState>>
    )

    /**
     * Stats for channels.
     */
    data class StreamStat(val timestamp: Instant)

    override fun run() {
        while (true) {
            val operatorStats = streamDAG.map { operator ->
                OperatorStat(
                    name = operator.name,
                    parallelism = operator.parallelism,
                    taskStats = operator.deployedTasks.map { (taskID, deployment) ->
                        Pair(taskID, deployment.state())
                    }
                )
            }
            val json = gson.toJson(operatorStats)
            println("OperatorStats\t$json")
            Thread.sleep(500)
        }
    }
}