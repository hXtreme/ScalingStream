package org.example.scalingstream.channels.local

import org.example.scalingstream.channels.ChannelArg
import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.channels.InputChannel
import org.example.scalingstream.channels.Record
import java.util.*
import java.util.concurrent.BlockingQueue

class LocalInputChannel<Type>(
    id: ChannelID,
    private val channelArgs: Map<ChannelArg, Any>
) : InputChannel<Type>(id) {
    private var queueDict: HashMap<ChannelID, BlockingQueue<Record<Type>?>> =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as HashMap<ChannelID, BlockingQueue<Record<Type>?>>
    private var q: BlockingQueue<Record<Type>?> =
        queueDict[id] ?: error("No queue named $id to connect to.")

    var isClosed: Boolean = false
        private set

    override fun peek(): Record<Type>? {
        return q.peek()
    }

    override fun get(): Record<Type> {
        val data = q.take()
        if (data is Nothing) {
            error("Deal with this")
        }
        return data!!
    }

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun connect() {}
}