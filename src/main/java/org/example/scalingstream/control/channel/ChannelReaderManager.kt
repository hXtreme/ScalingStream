package org.example.scalingstream.control.channel

import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.channels.ChannelReader
import org.example.scalingstream.channels.Record
import org.example.scalingstream.extensions.*

abstract class ChannelReaderManager<Type> : ChannelIOManager<Type> {
    private val channelReaderMap: MutableMap<ChannelID, ChannelReader<Type>> = mutableMapOf()
    private val inputChannelsIDIter = sequence {
        var current = 0
        while (true) {
            val inputChannelIDs = channelReaderMap.keys.toList()
            current %= channelReaderMap.size
            val k = inputChannelIDs[current]
            yield(Pair(k, channelReaderMap[k]!!))
            current++
        }
    }.iterator()

    fun isReady(): Boolean {
        return channelReaderMap.any { (_, v) ->
            v.peek() != null
        }
    }

    fun get(): Record<Type> {
        while (channelReaderMap.isEmpty()) {
            Thread.sleep(100)
        }

        if (closedAndEmpty) error("All channels are closed and there are no values to get")
        return (inputChannelsIDIter.take(channelReaderMap.size)
            .find { (_, v) -> v.peek() != null } ?: inputChannelsIDIter.first()).second
            .get()

    }

    abstract var closedAndEmpty: Boolean

    fun addChannels(channelReaders: List<ChannelReader<Type>>) {
        channelReaders.forEach { addChannel(it) }
    }

    fun addChannel(channelReader: ChannelReader<Type>) {
        val id = channelReader.id
        if (channelReaderMap.contains(id)) error("A channel already exists from task(${id.src}) to task(${id.dst})")
        channelReaderMap[id] = channelReader
    }

    fun removeChannel(id: ChannelID) {
        // TODO: Tell channelManager to close up a channel.
        channelReaderMap[id]!!.close()
        channelReaderMap.remove(id)
    }
}
