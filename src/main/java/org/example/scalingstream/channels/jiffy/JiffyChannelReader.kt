package org.example.scalingstream.channels.jiffy

import org.example.scalingstream.channels.*

class JiffyChannelReader<Type>(
    name: ChannelID, channelArgs: ChannelArgs
) : ChannelReader<Type>(name) {
    private val path = "/$name"

    private val host: String = channelArgs.getOrDefault(ChannelArg.JIFFY_HOST, "127.0.0.1") as String
    private val servicePort: Int = channelArgs.getOrDefault(ChannelArg.JIFFY_SERVICE_PORT, 9090) as Int
    private val leasePort: Int = channelArgs.getOrDefault(ChannelArg.JIFFY_LEASE_PORT, 9091) as Int

    override val isClosed: Boolean
        get() = TODO("Not yet implemented")
    override val isNotClosed: Boolean
        get() = TODO("Not yet implemented")
    override val isNotEmpty: Boolean
        get() = TODO("Not yet implemented")
    override val isEmpty: Boolean
        get() = TODO("Not yet implemented")

    override fun connect() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun peek(): Record<Type>? {
        TODO("Not yet implemented")
    }

    override fun get(): Record<Type> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}