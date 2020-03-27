package org.example.scalingstream.control.channel

import de.jupf.staticlog.Log
import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.channels.ChannelWriter
import org.example.scalingstream.channels.Record
import org.example.scalingstream.partitioner.PartitionerConstructor
import java.time.Instant

class BufferedChannelWriterManager<Type>(
    val batchSize: Int,
    partitioner: PartitionerConstructor
) : ChannelWriterManager<Type>(partitioner()) {
    private val buffers: MutableMap<ChannelID, MutableList<Type>> = mutableMapOf()
    private val channelIDs: List<ChannelID>
        get() {
            return channelWriters.keys.toList()
        }

    override var timestamp: Instant? = null
        set(value) {
            if (value != null && value != field) flushBuffers()
            field = value
        }

    override fun put(batch: List<Type>) {
        // TODO("Test")
        while (channelWriters.isEmpty()) {
            Log.debug("Putting thread to sleep")
            Thread.sleep(100)
        }
        val channels = channelIDs

        batch
            .groupBy { channels[partitioner.assignPartition(it, channels.size)] }
            .forEach { (id, records) -> buffers.getOrPut(id, ::mutableListOf).addAll(records) }

        buffers.filterValues { v -> (v.size >= batchSize) }.keys.forEach { flushBuffer(it) }
    }

    override fun close() {
        for (channelID in channelIDs) {
            close(channelID)
        }
    }

    fun close(channelID: ChannelID) {
        flushBuffer(channelID, channelWriters[channelID]!!)
        closeChannel(channelID)
    }

    override fun closeChannel(id: ChannelID) {
        channelWriters.remove(id)?.run {
            flushBuffer(id, this)
            buffers.remove(id) ?: Log.warn("This should not have been null", id.toString())
            this.close(timestamp)
        } ?: Log.warn(
            "Ideally we'd want two functions close(to close an open channel) and remove(for cleaning up a closed channel)."
        )
    }

    private fun flushBuffers() {
        channelWriters.filterKeys { key -> key in buffers.keys }.forEach { (k, v) -> flushBuffer(k, v) }
        timestamp = null
    }

    private fun flushBuffer(id: ChannelID, channelWriter: ChannelWriter<Type> = channelWriters[id]!!) {
        channelWriter.put(Record(timestamp, buffers[id]!!))
        buffers[id]!!.removeAll { true }
    }

    override fun addChannel(channelWriter: ChannelWriter<Type>) {
        super.addChannel(channelWriter)
        val id = channelWriter.id
        buffers[id] = mutableListOf()
    }
}
