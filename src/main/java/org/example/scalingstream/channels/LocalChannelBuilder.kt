package org.example.scalingstream.channels

import java.time.Instant
import java.util.*
import java.util.concurrent.BlockingQueue


object LocalChannelConstants {
    const val TYPE = "LOCAL"
}

open class LocalChannelBuilder(
    override val channelArgs: ChannelArgs
) : ChannelBuilder {
    override val type: String = LocalChannelConstants.TYPE

    override fun <Type> buildChannel(id: ChannelID): Channel<Type> {
        return LocalChannel(id, channelArgs)
    }
}

class LocalInputChannel<Type>(
    id: ChannelID,
    private val channelArgs: Map<ChannelArg, Any>
) : InputChannel<Type>(id) {
    private var queueDict: HashMap<Pair<UUID, UUID>, BlockingQueue<Pair<Instant?, List<Type>>?>> =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as HashMap<Pair<UUID, UUID>, BlockingQueue<Pair<Instant?, List<Type>>?>>
    private var q: BlockingQueue<Pair<Instant?, List<Type>>?> =
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

    override fun connect() {}
}

class LocalOutputChannel<Type>(
    id: ChannelID,
    private val channelArgs: Map<ChannelArg, Any>
) : OutputChannel<Type>(id) {
    private var queueDict: HashMap<Pair<UUID, UUID>, BlockingQueue<Pair<Instant?, List<Type>?>>> =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as HashMap<Pair<UUID, UUID>, BlockingQueue<Pair<Instant?, List<Type>?>>>
    private var q: BlockingQueue<Pair<Instant?, List<Type>?>> = queueDict[id] ?: error("No queue named $id to connect to.")

    override fun put(recordBatch: Pair<Instant?, List<Type>>) {
        q.put(Pair(recordBatch.first, recordBatch.second.toList()))
    }

    override fun flush() {
        // Do nothing
    }

    override fun close(timestamp: Instant?) {
        TODO("Not yet implemented")
    }

    override fun connect() {}
}
