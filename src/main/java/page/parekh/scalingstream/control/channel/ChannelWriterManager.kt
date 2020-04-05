package page.parekh.scalingstream.control.channel

import de.jupf.staticlog.Log
import page.parekh.scalingstream.channels.ChannelID
import page.parekh.scalingstream.channels.ChannelWriter
import page.parekh.scalingstream.partitioner.Partitioner
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
    open fun closeChannel(id: ChannelID) {
        // TODO: Add some handover logic
        // IE.What to do to propagate the information that the channel is closed;
        // and how to handle future partitioning.
        (channelWriters[id]?.close(timestamp) )
            ?: run {
                Log.error("This should not have been null.")
                // error("Could not find $id in channelWriters")
            }
        channelWriters.remove(id)
    }
}
