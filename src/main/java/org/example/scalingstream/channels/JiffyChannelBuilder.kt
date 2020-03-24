package org.example.scalingstream.channels

import java.time.Instant
import java.util.*


object JiffyChannelConstants {
    const val TYPE = "JIFFY"
}

class JiffyChannelBuilder(
    override val channelArgs: ChannelArgs
) : ChannelBuilder {
    override val type: String = JiffyChannelConstants.TYPE

    init {
        channelArgs.putIfAbsent(ChannelArg.JIFFY_HOST, "127.0.0.1")
        channelArgs.putIfAbsent(ChannelArg.JIFFY_SERVICE_PORT, 9090)
        channelArgs.putIfAbsent(ChannelArg.JIFFY_LEASE_PORT, 9091)
    }

    override fun <Type> buildChannel(id: ChannelID): Channel<Type> {
        return JiffyChannel(id, channelArgs)
    }
}


class JiffyInputChannel<Type>(
    name: Pair<UUID, UUID>, channelArgs: ChannelArgs
) : InputChannel<Type>(name) {
    private val path = "/$name"

    private val host: String = channelArgs.getOrDefault(ChannelArg.JIFFY_HOST, "127.0.0.1") as String
    private val servicePort: Int = channelArgs.getOrDefault(ChannelArg.JIFFY_SERVICE_PORT, 9090) as Int
    private val leasePort: Int = channelArgs.getOrDefault(ChannelArg.JIFFY_LEASE_PORT, 9091) as Int

    override fun connect() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun peek(): Record<Type>? {
        TODO("Not yet implemented")
    }

    override fun get(): Record<Type> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}


class JiffyOutputChannel<Type>(
    name: Pair<UUID, UUID>, channelArgs: ChannelArgs
) : OutputChannel<Type>(name) {
    private val path = "/$name"

    private val host: String = channelArgs.getOrDefault(ChannelArg.JIFFY_HOST, "127.0.0.1") as String
    private val servicePort: Int = channelArgs.getOrDefault(ChannelArg.JIFFY_SERVICE_PORT, 9090) as Int
    private val leasePort: Int = channelArgs.getOrDefault(ChannelArg.JIFFY_LEASE_PORT, 9091) as Int

    override fun connect() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun put(recordBatch: Record<Type>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun flush() {
        // Do nothing
    }

    override fun close(timestamp: Instant?) {
        TODO("Not yet implemented")
    }
}
