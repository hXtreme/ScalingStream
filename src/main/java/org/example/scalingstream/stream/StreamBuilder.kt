package org.example.scalingstream.stream

import de.jupf.staticlog.Log
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.dag.Operator
import org.example.scalingstream.executor.Executor
import org.example.scalingstream.operator.TaskConstructor
import org.example.scalingstream.partitioner.PartitionerConstructor
import org.jgrapht.graph.DirectedAcyclicGraph
import org.jgrapht.traverse.TopologicalOrderIterator

class StreamBuilder(private val channelBuilder: ChannelBuilder) {
    var streamExecutionDAG: DirectedAcyclicGraph<Operator<*, *, *, *>, ChannelManager<*>> =
        DirectedAcyclicGraph(ChannelManager::class.java)
    private val roots: HashSet<Operator<*, *, *, *>> = HashSet()

    fun <OutputType : Any> addSource(
        name: String,
        task: TaskConstructor<Unit, Unit, OutputType?, OutputType>,
        batchSize: Int,
        initialParallelism: Int,
        partitioner: PartitionerConstructor,
        operatorFn: (Unit) -> OutputType?
    ): Operator<Unit, Unit, OutputType?, OutputType> {
        Log.info("Add Source Operator:\t$name\t$initialParallelism", name)
        val operator = Operator(name, this, task, batchSize, initialParallelism, partitioner, operatorFn)
        roots.add(operator)
        streamExecutionDAG.addVertex(operator)
        return operator
    }

    fun <InputType, FnIn, FnOut, OutputType> addOperator(
        parent: Operator<*, *, *, InputType>,
        name: String,
        task: TaskConstructor<InputType, FnIn, FnOut, OutputType>,
        batchSize: Int,
        initialParallelism: Int,
        partitioner: PartitionerConstructor,
        operatorFn: (FnIn) -> FnOut
    ): Operator<InputType, FnIn, FnOut, OutputType> {
        Log.info("Add operator:\t$name\t\t$initialParallelism", name)
        if (!streamExecutionDAG.containsVertex(parent))
            error("The parent Operator($parent) does not exist in the StreamExecutionDAG")
        val channelManager = ChannelManager<InputType>(channelBuilder, batchSize, partitioner)
        val operator = Operator(name, this, task, batchSize, initialParallelism, partitioner, operatorFn)
        streamExecutionDAG.addVertex(operator)
        streamExecutionDAG.addEdge(parent, operator, channelManager)
        return operator
    }

    fun run(executor: Executor) {
        // TODO("Run")
        Log.info("Starting Stream execution")
        executor.exec(streamExecutionDAG)
        streamExecutionDAG.edgeSet().forEach { it.destroy() }
    }
}
