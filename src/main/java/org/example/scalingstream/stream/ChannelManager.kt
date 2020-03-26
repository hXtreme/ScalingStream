package org.example.scalingstream.stream

import org.example.scalingstream.channels.*
import org.example.scalingstream.control.channel.ChannelReaderManager
import org.example.scalingstream.control.channel.ChannelWriterManager
import org.example.scalingstream.dag.Operator
import org.example.scalingstream.partitioner.PartitionerConstructor
import org.jgrapht.graph.DefaultEdge
import java.util.*
import kotlin.collections.HashMap


class ChannelManager<Type>(
    val channelBuilder: ChannelBuilder,
    val batchSize: Int,
    val partitionerConstructor: PartitionerConstructor
) : DefaultEdge() {
    val channels: MutableMap<ChannelID, Channel<Type>> = HashMap()
    private val channelWriterManagers: MutableMap<UUID, ChannelWriterManager<Type>> = HashMap()
    private val channelReaderManagers: MutableMap<UUID, ChannelReaderManager<Type>> = HashMap()

    @Suppress("UNCHECKED_CAST")
    val src: Operator<*, *, *, Type>
        get() = source as Operator<*, *, *, Type>

    @Suppress("UNCHECKED_CAST")
    val dst: Operator<Type, *, *, *>
        get() = target as Operator<Type, *, *, *>

    /**
     * Build a channel that transfers data from src([UUID]) to dst([UUID])
     * @return the ChannelBuilder for the channel that can be used to make Input and Output endpoints.
     */
    fun channelBuilder(id: ChannelID): Channel<Type> {
        // TODO("Setup Channel-stuff")
        if (channels.contains(id)) error("A channel already exists from task(${id.src}) to task(${id.dst})")
        channels[id] = channelBuilder.buildChannel(id)

        return channels[id]!!
    }

    val isClosedAndEmpty: Boolean
        get() = false // TODO("Not yet implemented")

    fun destroy() {
        // TODO("Not yet implemented")
        channels.keys.forEach { destroy(it) }
    }

    fun destroy(id: ChannelID) {
        channels.remove(id)
    }


    fun registerChannelWriteManager(taskID: UUID, channelWriterManager: ChannelWriterManager<Type>) {
        if (channelWriterManagers.contains(taskID)) error("There already exist a ChannelWriteManager associated with task($taskID)")
        channelWriterManagers[taskID] = channelWriterManager
    }

    fun registerChannelReadManager(taskID: UUID, channelReaderManager: ChannelReaderManager<Type>) {
        if (channelReaderManagers.contains(taskID)) error("There already exist a ChannelReadManager associated with task($taskID)")
        channelReaderManagers[taskID] = channelReaderManager
    }

    fun addChannelWrite(channelID: ChannelID, channelWriter: ChannelWriter<Type>) {
        // TODO("Not yet implemented")
        channelWriterManagers[channelID.src]!!.addChannel(channelWriter)
    }

    fun addChannelRead(channelID: ChannelID, channelReader: ChannelReader<Type>) {
        // TODO("Not yet implemented")
        channelReaderManagers[channelID.dst]!!.addChannel(channelReader)
    }
}
