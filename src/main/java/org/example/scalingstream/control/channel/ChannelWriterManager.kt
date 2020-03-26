package org.example.scalingstream.control.channel

import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.channels.ChannelWriter
import org.example.scalingstream.partitioner.Partitioner
import java.time.Instant

abstract class ChannelWriterManager<Type>(val partitioner: Partitioner) : ChannelIOManager<Type> {
    val channelWriters: MutableMap<ChannelID, ChannelWriter<Type>> = mutableMapOf()

    abstract fun put(batch: List<Type>)

    abstract var timestamp: Instant?

    fun addChannels(channelWriters: List<ChannelWriter<Type>>) {
        channelWriters.forEach { addChannel(it) }
    }

    open fun addChannel(channelWriter: ChannelWriter<Type>) {
        val id = channelWriter.id
        if (channelWriters.contains(id)) error("A channel already exists from task(${id.src}) to task(${id.dst})")
        channelWriters[id] = channelWriter
    }

    /**
     * Should only ever be called from ChannelManager.
     */
    open fun removeChannel(id: ChannelID) {
        // TODO: Add some handover logic
        // IE.What to do to propagate the information that the channel is closed;
        // and how to handle future partitioning.
        channelWriters[id]!!.close(timestamp)
        channelWriters.remove(id)
    }
}