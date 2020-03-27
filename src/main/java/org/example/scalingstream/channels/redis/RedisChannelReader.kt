package org.example.scalingstream.channels.redis

import org.example.scalingstream.channels.*

class RedisChannelReader<Type>(
    name: ChannelID,
    channelArgs: ChannelArgs
) : ChannelReader<Type>(name) {

    private val host: String = channelArgs.getOrDefault(ChannelArg.REDIS_HOST, "127.0.0.1") as String
    private val port: Int = channelArgs.getOrDefault(ChannelArg.REDIS_PORT, 6379) as Int
    private val db: Int = channelArgs.getOrDefault(ChannelArg.REDIS_DB, 0) as Int

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