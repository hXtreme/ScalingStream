package page.parekh.scalingstream.stream

import de.jupf.staticlog.Log
import page.parekh.scalingstream.channels.ChannelBuilder
import page.parekh.scalingstream.dag.Operator
import page.parekh.scalingstream.executor.Executor
import page.parekh.scalingstream.operator.TaskConstructor
import page.parekh.scalingstream.partitioner.PartitionerConstructor
import org.jgrapht.graph.DirectedAcyclicGraph

typealias StreamExecutionDAG = DirectedAcyclicGraph<Operator<*, *, *, *>, ChannelManager<*>>

class StreamBuilder(private val channelBuilder: ChannelBuilder) {
    var streamExecutionDAG: StreamExecutionDAG =
        StreamExecutionDAG(ChannelManager::class.java)
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
        with(Thread(StreamMonitor(streamExecutionDAG))) {
            this.isDaemon = true
            this.start()
        }
        executor.exec(streamExecutionDAG)
        streamExecutionDAG.edgeSet().forEach { it.destroy() }
    }
}
