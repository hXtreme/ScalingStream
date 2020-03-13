package org.example.scalingstream.stream

import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.channels.DataChannelContext
import org.example.scalingstream.channels.InputChannel
import org.example.scalingstream.channels.OutputChannel
import org.example.scalingstream.operator.OutputBuffers
import org.example.scalingstream.partitioner.Partitioner
import org.jgrapht.graph.DefaultEdge
import java.time.Instant


class ChannelManger<Type>() : DefaultEdge() {
    lateinit var channelBuilder: ChannelBuilder
    lateinit var partitionerConstructor: (Int) -> Partitioner
    lateinit var channelContext: DataChannelContext
    var batchSize = 0
    lateinit var outputBuffers: OutputBuffers<Type>

    val src: Node<*, *, *, *>
        get() = source as Node<*, *, *, *>

    val dst: Node<*, *, *, *>
        get() = target as Node<*, *, *, *>

    fun build(name: String): ChannelBuilder {
        channelContext = channelBuilder.buildChannelContext<Type>(name)

        return channelBuilder
        // TODO("Setup Channel-stuff")
    }

    fun destroy() {
        // TODO("Not yet implemented")
        channelContext.destroy()
    }

    constructor(channelBuilder: ChannelBuilder, batchSize: Int, partitionerConstructor: (Int) -> Partitioner) : this() {
        this.channelBuilder = channelBuilder
        this.batchSize = batchSize
        this.partitionerConstructor = partitionerConstructor
    }


    var channels: MutableMap<String, MutableMap<String, Triple<DataChannelContext, InputChannel<Type>, OutputChannel<Type>>>> =
        HashMap() // TODO("Find a better Data Structure")
    var inputChannels: MutableMap<Pair<String, String>, InputChannel<Type>> = HashMap()
    var outputChannels: MutableMap<Pair<String, String>, OutputChannel<Type>> = HashMap()
    var channelContexts: MutableMap<Pair<String, String>, DataChannelContext> = HashMap()

    fun put(id: String, recordBatch: Pair<Instant?, List<Type>?>) {
//        val outputChannels = channels[id]!!.map { it.value.third }
        val outputChannels = outputChannels.filterKeys { (src, _) -> src == id}.map {it.value} // TODO: Find a reliable way
        val partitioner = partitionerConstructor(outputChannels.size)

        outputBuffers = OutputBuffers<Type>(batchSize, outputChannels)
        outputBuffers.append(recordBatch, partitioner) // TODO: Think of a better way
    }

    fun get(id: String): Pair<Instant?, List<Type>?> {
        // TODO: This is too much of a bodge, do something smarter **(Queue of queues)**
        // Select one of the many input channel to choose from
        val inputChannels = inputChannels.filterKeys {(_, dst) -> dst == id}.map { it.value }

        return (inputChannels.find { it.peek() != null }?: inputChannels.first()).get()

//        val inputChannels = channels.map { (k, v) -> Pair(k, v[id]!!.second) }


    }

    private val producers: MutableSet<String> = HashSet()

    private val consumers: MutableSet<String> = HashSet()

    fun connectProducer(id: String) {
        producers.add(id)
        val pairs = consumers.map { Pair(id, it) }
        pairs.forEach {
            channelBuilder.buildChannelContext<Type>(it.toString())
            inputChannels[it] = channelBuilder.buildInputChannel(it.toString())
            outputChannels[it] = channelBuilder.buildOutputChannel(it.toString())
        }
    }

    //    @Synchronized
    fun connectConsumer(id: String) {
        consumers.add(id)
        val pairs = producers.map { Pair(it, id) }
        pairs.forEach {
            channelBuilder.buildChannelContext<Type>(it.toString())
            inputChannels[it] = channelBuilder.buildInputChannel(it.toString())
            outputChannels[it] = channelBuilder.buildOutputChannel(it.toString())
        }
    }
}