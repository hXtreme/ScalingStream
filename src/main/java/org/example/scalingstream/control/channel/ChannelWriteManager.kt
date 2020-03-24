package org.example.scalingstream.control.channel

import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.channels.OutputChannel
import org.example.scalingstream.partitioner.Partitioner
import java.time.Instant

abstract class ChannelWriteManager<Type>(val partitioner: Partitioner) : ChannelEndPointManager<Type> {
    val outputChannels: MutableMap<ChannelID, OutputChannel<Type>> = mutableMapOf()

    abstract fun put(batch: List<Type>)

    abstract var timestamp: Instant?

    fun addChannels(channels: List<OutputChannel<Type>>) {
        channels.forEach { addChannel(it) }
    }

    open fun addChannel(channel: OutputChannel<Type>) {
        val id = channel.id
        if (outputChannels.contains(id)) error("A channel already exists from task(${id.first}) to task(${id.second})")
        outputChannels[id] = channel
    }

    /**
     * Should only ever be called from ChannelManager.
     */
    open fun removeChannel(id: ChannelID) {
        // TODO: Add some handover logic
        // IE.What to do to propagate the information that the channel is closed;
        // and how to handle future partitioning.
        outputChannels[id]!!.close(timestamp)
        outputChannels.remove(id)
    }
}
