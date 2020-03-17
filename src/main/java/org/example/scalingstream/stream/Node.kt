package org.example.scalingstream.stream

import de.jupf.staticlog.Log
import org.example.scalingstream.control.channel.InputChannelManager
import org.example.scalingstream.control.channel.OutputChannelManager
import org.example.scalingstream.operator.Task
import org.example.scalingstream.operator.TaskConstructor
import org.example.scalingstream.partitioner.Partitioner

class Node<InputType, FnIn, FnOut, OutputType>(
    private val streamBuilder: StreamBuilder,
    private val name: String,
    private val task: TaskConstructor<InputType, FnIn, FnOut, OutputType>,
    private val outChannelManager: ChannelManager<OutputType>,
    private val batchSize: Int,
    private val parallelism: Int,
    private val partitioner: (Int) -> Partitioner,
    private val operatorFn: (FnIn) -> FnOut
) {

    fun <FnIn, FnOut, Type> addOperator(
        name: String,
        task: TaskConstructor<OutputType, FnIn, FnOut, Type>,
        batchSize: Int,
        parallelism: Int,
        partitioner: (Int) -> Partitioner,
        operatorFn: (FnIn) -> FnOut
    ): Node<OutputType, FnIn, FnOut, Type> {
        return streamBuilder.addOperator(this, name, task, batchSize, parallelism, partitioner, operatorFn)
    }

    private val uID = StaticVars.uID++
    val operatorID = "_${name}_operator_${uID}_"

    fun run() {
        Log.info("Running $operatorID")
        val tasks: MutableList<Thread> = ArrayList()
        for (i in 0 until parallelism) {
            tasks.add(
                Thread( { build(i).run() }, "$operatorID${i}_")
            )
        }
        tasks.forEach { it.start() }
        tasks.forEach { it.join() }
    }

    fun build(id: Int): Task<InputType, *, *, OutputType> {
        val upstreamCount= fun() : Int {
            return streamBuilder.streamExecutionDAG.incomingEdgesOf(this).map { it.src.parallelism }.sum()
        }

        val inChannelManagers = fun(): List<InputChannelManager<InputType>> {
            TODO("not implemented")
        }

        val outChannelManagers = fun(): List<OutputChannelManager<OutputType>> {
            TODO("not implemented")
        }

        // TODO("Operators shouldn't need outOperatorIDs, because in our case operator is actually just a task")
        // TODO("Operators shouldn't keep upstream count as val, we want to change it dynamically")
        // TODO("Potential write before read with parallelism, use Sentinel value instead to mitigate this.")
        return task(id, operatorID, inChannelManagers(), outChannelManagers(), operatorFn)
    }

    companion object StaticVars {
        var uID: Int = 0
    }


    override fun toString(): String {
        return operatorID
    }

    override fun equals(other: Any?): Boolean {
        return other is Node<*, *, *, *> && (toString() == other.toString() && task.toString() == other.task.toString())
    }

    override fun hashCode(): Int {
        return (toString() + task.toString()).hashCode()
    }
}
