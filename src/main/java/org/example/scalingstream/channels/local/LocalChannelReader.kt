package org.example.scalingstream.channels.local

import org.example.scalingstream.channels.ChannelArg
import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.channels.ChannelReader
import org.example.scalingstream.channels.Record
import java.util.*

class LocalChannelReader<Type>(
    id: ChannelID,
    private val channelArgs: Map<ChannelArg, Any>
) : ChannelReader<Type>(id) {
    private var queueDict: HashMap<ChannelID, CloseableLinkedBlockingQueue<Record<Type>>> =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as HashMap<ChannelID, CloseableLinkedBlockingQueue<Record<Type>>>
    private var q: CloseableLinkedBlockingQueue<Record<Type>> =
        queueDict[id] ?: error("No queue named $id to connect to.")

    override val isClosed: Boolean
        get() {
            return q.isClosed
        }
    override val isNotClosed: Boolean
        get() {
            return !q.isClosed
        }
    override val isEmpty: Boolean
        get() {
            return q.isEmpty()
        }
    override val isNotEmpty: Boolean
        get() {
            return q.isNotEmpty()
        }

    override fun peek(): Record<Type>? {
        return q.peek()
    }

    override fun get(): Record<Type> {
        if (isClosed && isEmpty) error("Can't read from an empty closed channel.")
        return q.take()
    }

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun connect() {}
}
