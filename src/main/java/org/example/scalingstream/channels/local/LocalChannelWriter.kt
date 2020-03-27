package org.example.scalingstream.channels.local

import org.example.scalingstream.channels.ChannelArg
import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.channels.ChannelWriter
import org.example.scalingstream.channels.Record
import java.time.Instant
import java.util.*

class LocalChannelWriter<Type>(
    id: ChannelID,
    private val channelArgs: Map<ChannelArg, Any>
) : ChannelWriter<Type>(id) {
    private var queueDict: HashMap<ChannelID, CloseableLinkedBlockingQueue<Record<Type>>> =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as HashMap<ChannelID, CloseableLinkedBlockingQueue<Record<Type>>>
    private var q: CloseableLinkedBlockingQueue<Record<Type>> = queueDict[id] ?: error("No queue named $id to connect to.")

    override fun put(recordBatch: Record<Type>) {
//        Log.debug("Writing $recordBatch to LocalChannel")
        val (timestamp, batch) = recordBatch
        val copiedBatch = batch.map { it }
        q.put(Record(timestamp, copiedBatch))
    }

    override fun flush() {
        // Do nothing
    }

    override fun close(timestamp: Instant?) {
        q.close()
    }

    override fun connect() {}
}