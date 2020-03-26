package org.example.scalingstream.stream

import org.example.scalingstream.channels.*
import org.example.scalingstream.control.channel.ChannelReadManager
import org.example.scalingstream.control.channel.ChannelWriteManager
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
    private val channelWriteManagers: MutableMap<UUID, ChannelWriteManager<Type>> = HashMap()
    private val channelReadManagers: MutableMap<UUID, ChannelReadManager<Type>> = HashMap()

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
        if (channels.contains(id)) error("A channel already exists from task(${id.first}) to task(${id.second})")
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


    fun registerChannelWriteManager(taskID: UUID, channelWriteManager: ChannelWriteManager<Type>) {
        if (channelWriteManagers.contains(taskID)) error("There already exist a ChannelWriteManager associated with task($taskID)")
        channelWriteManagers[taskID] = channelWriteManager
    }

    fun registerChannelReadManager(taskID: UUID, channelReadManager: ChannelReadManager<Type>) {
        if (channelReadManagers.contains(taskID)) error("There already exist a ChannelReadManager associated with task($taskID)")
        channelReadManagers[taskID] = channelReadManager
    }

    fun addChannelWrite(channelID: ChannelID, outputChannel: OutputChannel<Type>) {
        // TODO("Not yet implemented")
        channelWriteManagers[channelID.first]!!.addChannel(outputChannel)
    }

    fun addChannelRead(channelID: ChannelID, inputChannel: InputChannel<Type>) {
        // TODO("Not yet implemented")
        channelReadManagers[channelID.second]!!.addChannel(inputChannel)
    }
}
