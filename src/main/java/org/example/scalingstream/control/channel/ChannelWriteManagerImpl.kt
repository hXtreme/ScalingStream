package org.example.scalingstream.control.channel

import de.jupf.staticlog.Log
import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.channels.Record
import org.example.scalingstream.partitioner.PartitionerConstructor
import java.time.Instant

class ChannelWriteManagerImpl<Type>(
    val batchSize: Int,
    partitioner: PartitionerConstructor
) : ChannelWriteManager<Type>(partitioner()) {
    private val buffers: MutableMap<ChannelID, MutableList<Type>> = mutableMapOf()
    private val channelIDs
        get() = outputChannels.keys.toList()

    override var timestamp: Instant? = null
        set(value) {
            if (value != null) flushBuffers()
            field = value
        }

    override fun put(batch: List<Type>) {
        // TODO("Test")
        while (outputChannels.isEmpty()) {
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
        flushBuffers()
        for (channelID in outputChannels.keys) {
            close(channelID)
        }
    }

    fun close(channelID: ChannelID) {
        flushBuffer(channelID)
        removeChannel(channelID)
    }

    override fun removeChannel(id: ChannelID) {
        buffers.remove(id)
        super.removeChannel(id)
    }

    private fun flushBuffers() {
        buffers.keys.forEach { flushBuffer(it) }
        timestamp = null
    }

    private fun flushBuffer(id: ChannelID) {
        // TODO("Test")
        outputChannels[id]!!.put(Record(timestamp, buffers[id]!!))
        buffers[id]!!.removeAll { true }
    }
}