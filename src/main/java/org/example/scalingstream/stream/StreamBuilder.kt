package org.example.scalingstream.stream

import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.operator.OperatorConstructor
import org.example.scalingstream.operator.SimpleOperatorConstructor
import org.example.scalingstream.partitioner.Partitioner
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.DirectedAcyclicGraph

class StreamBuilder(private val channelBuilder: ChannelBuilder) {
    private lateinit var streamExecutionDAG: DirectedAcyclicGraph<Node<*, *>, ChannelManger>
    private val roots: HashSet<Node<*, *>> = HashSet()

    fun <OutputType> addSource(
        name: String,
        operator: SimpleOperatorConstructor<Unit, OutputType>,
        batchSize: Int,
        parallelism: Int,
        partitioner: (Int) -> Partitioner,
        operatorFn: (Unit) -> OutputType
    ): Node<Unit, OutputType> {
        val manager = Node(SimpleOperatorManager(name, operator, batchSize, parallelism, partitioner, operatorFn), this)
        roots.add(manager)
        streamExecutionDAG.addVertex(manager)
        return manager
    }

    fun <InputType, FnIn, FnOut, OutputType> addOperator(
        parent: Node<*, InputType>,
        name: String,
        operator: OperatorConstructor<InputType, FnIn, FnOut, OutputType>,
        batchSize: Int,
        parallelism: Int,
        partitioner: (Int) -> Partitioner,
        operatorFn: (FnIn) -> FnOut
    ): Node<InputType, OutputType> {
        if (!streamExecutionDAG.containsVertex(parent))
            error("The parent OperatorManager does not exist in the StreamExecutionDAG")
        val manager = Node(OperatorManager(name, operator, batchSize, parallelism, partitioner, operatorFn), this)
        streamExecutionDAG.addEdge(parent, manager)
        return manager
    }

    fun run() {

    }
}

private typealias SimpleOperatorManager<InputType, OutputType> =
        OperatorManager<InputType, InputType, OutputType, OutputType>

class OperatorManager<InputType, FnIn, FnOut, OutputType>(
    name: String,
    operator: OperatorConstructor<InputType, FnIn, FnOut, OutputType>,
    batchSize: Int,
    parallelism: Int,
    partitioner: (Int) -> Partitioner,
    operatorFn: (FnIn) -> FnOut
) {

}

class Node<InputType, OutputType>(
    val value: OperatorManager<InputType, *, *, OutputType>,
    private val streamBuilder: StreamBuilder
) {

    fun <FnIn, FnOut, Type> addOperator(
        name: String,
        operator: OperatorConstructor<OutputType, FnIn, FnOut, Type>,
        batchSize: Int,
        parallelism: Int,
        partitioner: (Int) -> Partitioner,
        operatorFn: (FnIn) -> FnOut
    ): Node<OutputType, Type> {
        return streamBuilder.addOperator(this, name, operator, batchSize, parallelism, partitioner, operatorFn)
    }
}

class ChannelManger(private val source: Node<*, *>, private val target: Node<*, *>) :
    DefaultEdge() {

    override fun getTarget(): Any {
        return target
    }

    override fun getSource(): Any {
        return source
    }
}
