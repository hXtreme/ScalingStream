package org.example.scalingstream.stream

import de.jupf.staticlog.Log
import org.example.scalingstream.channels.*
import org.example.scalingstream.control.channel.BufferedChannelWriterManager
import org.example.scalingstream.control.channel.ChannelReaderManager
import org.example.scalingstream.control.channel.ChannelReaderManagerImpl
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
        if (channels.contains(id)) error("A channel already exists from task(${id.src}) to task(${id.dst})")
        channels[id] = channelBuilder.buildChannel(id)
        return channels[id]!!
    }

    fun addChannelReader(channelID: ChannelID) {
        val channel = channels.getOrPut(channelID) { channelBuilder.buildChannel(channelID) }
        val channelReader = channel.getChannelReader()
        channelReaderManagers.getOrPut(channelID.dst) { ChannelReaderManagerImpl() }.addChannel(channelReader)
    }

    fun addChannelWriter(channelID: ChannelID) {
        val channel = channels.getOrPut(channelID) { channelBuilder.buildChannel(channelID) }
        val channelWriter = channel.getChannelWriter()
        channelWriterManagers.getOrPut(channelID.src) { BufferedChannelWriterManager(batchSize, partitionerConstructor) }
            .addChannel(channelWriter)
    }

    fun destroy() {
        // TODO("Not yet implemented")
        channels.keys.forEach { destroy(it) }
    }

    fun destroy(id: ChannelID) {
        /**
         * 1. Stop Writer
         * 2. Remove Writer
         */
        val channel = channels.remove(id)
        channelWriterManagers.remove(id.src)?.closeChannel(id) ?: Log.warn("Ideally we'd want two functions destroy and remove.")
    }


    fun registerChannelWriteManager(taskID: UUID, channelWriterManager: ChannelWriterManager<Type>) {
        if (channelWriterManagers.contains(taskID)) error("There already exist a ChannelWriteManager associated with task($taskID)")
        channelWriterManagers[taskID] = channelWriterManager
    }

    fun registerChannelReadManager(taskID: UUID, channelReaderManager: ChannelReaderManager<Type>) {
        if (channelReaderManagers.contains(taskID)) error("There already exist a ChannelReadManager associated with task($taskID)")
        channelReaderManagers[taskID] = channelReaderManager
    }

}
