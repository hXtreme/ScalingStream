package page.parekh.scalingstream

import page.parekh.scalingstream.channels.*
import page.parekh.scalingstream.stream.StreamBuilder
import page.parekh.scalingstream.channels.local.LocalChannelConstants.TYPE as LOCAL_TYPE
import page.parekh.scalingstream.executor.Executor
import page.parekh.scalingstream.operator.Source
import page.parekh.scalingstream.partitioner.PartitionerConstructor
import page.parekh.scalingstream.partitioner.RoundRobinPartitioner
import page.parekh.scalingstream.stream.Stream
import java.util.*
import kotlin.collections.HashMap

class StreamContext(
    private val executor: Executor,
    private val channelBuilder: ChannelBuilder,
    channelArgs: ChannelArgs,
    private val defaultBatchSize: Int = 1,
    private val defaultPartitioner: PartitionerConstructor = ::RoundRobinPartitioner
) {
    private val streamBuilder = StreamBuilder(channelBuilder)

    init {
        if (channelBuilder.type == LOCAL_TYPE) {
            if (executor.type != LOCAL_TYPE)
                error("Local channel can only be used with local executor, provided executor: ${executor.type}")
            channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Any>>()
        }
    }

    fun <Type: Any> createStream(
        name: String,
        batchSize: Int = defaultBatchSize,
        parallelism: Int = 1,
        partitioner: PartitionerConstructor = defaultPartitioner,
        generatorFn: (Unit) -> Type?
    ): Stream<Unit, Type> {
        val executionNode = streamBuilder.addSource(name, ::Source, batchSize, parallelism, partitioner, generatorFn)
        return Stream(executionNode, batchSize, parallelism, partitioner)
    }

    fun run() {
        // TODO("run the master")
        streamBuilder.run(executor)
    }
}
