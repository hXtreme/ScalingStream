package org.example.scalingstream.control.channel

import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.channels.InputChannel
import org.example.scalingstream.channels.Record
import org.example.scalingstream.extensions.*

abstract class ChannelReadManager<Type> : ChannelEndPointManager<Type> {
    private val inputChannelMap: MutableMap<ChannelID, InputChannel<Type>> = mutableMapOf()
    private val inputChannelsIDIter = sequence {
        var current = 0
        while (true) {
            val inputChannelIDs = inputChannelMap.keys.toList()
            current %= inputChannelMap.size
            val k = inputChannelIDs[current]
            yield(Pair(k, inputChannelMap[k]!!))
            current++
        }
    }.iterator()

    fun isReady(): Boolean {
        return inputChannelMap.any { (_, v) ->
            v.peek() != null
        }
    }

    fun get(): Record<Type> {
        while (inputChannelMap.isEmpty()) {
            Thread.sleep(100)
        }

        if (closedAndEmpty) error("All channels are closed and there are no values to get")
        return (inputChannelsIDIter.take(inputChannelMap.size)
            .find { (_, v) -> v.peek() != null } ?: inputChannelsIDIter.first()).second
            .get()

    }

    abstract var closedAndEmpty: Boolean

    fun addChannels(channels: List<InputChannel<Type>>) {
        channels.forEach { addChannel(it) }
    }

    fun addChannel(channel: InputChannel<Type>) {
        val id = channel.id
        if (inputChannelMap.contains(id)) error("A channel already exists from task(${id.src}) to task(${id.dst})")
        inputChannelMap[id] = channel
    }

    fun removeChannel(id: ChannelID) {
        // TODO: Tell channelManager to close up a channel.
        inputChannelMap[id]!!.close()
        inputChannelMap.remove(id)
    }
}
