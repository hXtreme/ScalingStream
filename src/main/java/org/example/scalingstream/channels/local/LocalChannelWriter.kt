package org.example.scalingstream.channels.local

import org.example.scalingstream.channels.ChannelArg
import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.channels.ChannelWriter
import org.example.scalingstream.channels.Record
import java.time.Instant
import java.util.*
import java.util.concurrent.BlockingQueue

class LocalChannelWriter<Type>(
    id: ChannelID,
    private val channelArgs: Map<ChannelArg, Any>
) : ChannelWriter<Type>(id) {
    private var queueDict: HashMap<ChannelID, CloseableLinkedBlockingQueue<Record<Type>>> =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as HashMap<ChannelID, CloseableLinkedBlockingQueue<Record<Type>>>
    private var q: CloseableLinkedBlockingQueue<Record<Type>> = queueDict[id] ?: error("No queue named $id to connect to.")

    override fun put(recordBatch: Record<Type>) {
        q.put(recordBatch)
    }

    override fun flush() {
        // Do nothing
    }

    override fun close(timestamp: Instant?) {
        // TODO("To implement this just extend Blocking queue.")
        q.put(null)
    }

    override fun connect() {}
}