package org.example.scalingstream.stream

import de.jupf.staticlog.Log
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.executor.Executor
import org.example.scalingstream.operator.TaskConstructor
import org.example.scalingstream.operator.SimpleTaskConstructor
import org.example.scalingstream.partitioner.Partitioner
import org.jgrapht.graph.DirectedAcyclicGraph

class StreamBuilder(private val channelBuilder: ChannelBuilder) {
    var streamExecutionDAG: DirectedAcyclicGraph<Node<*, *, *, *>, ChannelManger<*>> =
        DirectedAcyclicGraph(ChannelManger::class.java)
    private val roots: HashSet<Node<*, *, *, *>> = HashSet()

    fun <OutputType> addSource(
        name: String,
        task: SimpleTaskConstructor<Unit, OutputType>,
        batchSize: Int,
        parallelism: Int,
        partitioner: (Int) -> Partitioner,
        operatorFn: (Unit) -> OutputType
    ): Node<Unit, Unit, OutputType, OutputType> {
        Log.info("Add source:\t\t$name\t$parallelism")
        val manager =
            Node(
                this,
                name,
                task,
                ChannelManger(channelBuilder, batchSize, partitioner),
                batchSize,
                parallelism,
                partitioner,
                operatorFn
            )
        roots.add(manager)
        streamExecutionDAG.addVertex(manager)
        return manager
    }

    fun <InputType, FnIn, FnOut, OutputType> addOperator(
        parent: Node<*, *, *, InputType>,
        name: String,
        task: TaskConstructor<InputType, FnIn, FnOut, OutputType>,
        batchSize: Int,
        parallelism: Int,
        partitioner: (Int) -> Partitioner,
        operatorFn: (FnIn) -> FnOut
    ): Node<InputType, FnIn, FnOut, OutputType> {
        Log.info("Add operator:\t$name\t\t$parallelism")
        if (!streamExecutionDAG.containsVertex(parent))
            error("The parent OperatorManager does not exist in the StreamExecutionDAG")
        val inChannelManger = ChannelManger<OutputType>(channelBuilder, batchSize, partitioner)
        val operatorManager = Node(
            this,
            name,
            task,
            inChannelManger,
            batchSize,
            parallelism,
            partitioner,
            operatorFn
        )
        streamExecutionDAG.addVertex(operatorManager)
        streamExecutionDAG.addEdge(parent, operatorManager, inChannelManger)
        return operatorManager
    }

    fun run(executor: Executor) {

        // TODO("Run")
        // streamExecutionDAG.edgeSet().forEach { it.build() } // Initialize Channels for running
        Log.info("Starting Stream execution")
        executor.exec(streamExecutionDAG.vertexSet())
        streamExecutionDAG.edgeSet().forEach { it.destroy() }
    }
}
