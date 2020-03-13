package org.example.scalingstream

import org.example.scalingstream.channels.*
import org.example.scalingstream.stream.StreamBuilder
import org.example.scalingstream.channels.LocalChannelConstants.TYPE as LOCAL_TYPE
import org.example.scalingstream.executor.Executor
import org.example.scalingstream.operator.Source
import org.example.scalingstream.partitioner.Partitioner
import org.example.scalingstream.partitioner.RoundRobinPartitioner
import org.example.scalingstream.stream.Stream
import java.util.*
import kotlin.collections.HashMap

class StreamContext(
    executor: Executor,
    private val channelBuilder: ChannelBuilder,
    channelArgs: ChannelArgs,
    private val defaultBatchSize: Int = 1,
    private val defaultPartitioner: (Int) -> Partitioner = ::RoundRobinPartitioner
) {
    private val streamBuilder = StreamBuilder(channelBuilder)

    init {
        if (channelBuilder.type == LOCAL_TYPE) {
            if (executor.type != LOCAL_TYPE)
                error("Local channel can only be used with local executor, provided executor: ${executor.type}")
            channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Any>>()
        }
    }

    fun <Type> createStream(
        name: String,
        batchSize: Int = defaultBatchSize,
        parallelism: Int = 1,
        partitioner: (Int) -> Partitioner = defaultPartitioner,
        generatorFn: (Unit) -> Type
    ): Stream<Unit, Type> {
        val executionNode = streamBuilder.addSource(name, ::Source, batchSize, parallelism, partitioner, generatorFn)
        return Stream(executionNode, batchSize, parallelism, partitioner)
    }

    fun run() {
        // TODO("run the master")
        streamBuilder.run()
    }
}