package org.example.scalingstream.channels

import java.time.Instant
import java.util.*


object RedisChannelConstants {
    const val TYPE = "REDIS"
}


class RedisChannelBuilder(
    override val channelArgs: ChannelArgs
) : ChannelBuilder {
    override val type: String = RedisChannelConstants.TYPE

    init {
        channelArgs.putIfAbsent(ChannelArg.REDIS_HOST, "127.0.0.1") as String
        channelArgs.putIfAbsent(ChannelArg.REDIS_PORT, 6379) as Int
        channelArgs.putIfAbsent(ChannelArg.REDIS_DB, 0) as Int
    }

    override fun <Type> buildChannel(id: ChannelID): Channel<Type> {
        // TODO("Not yet implemented")
        return RedisChannel(id, channelArgs)
    }
}

class RedisInputChannel<Type>(
    name: ChannelID,
    channelArgs: ChannelArgs
) : InputChannel<Type>(name) {

    private val host: String = channelArgs.getOrDefault(ChannelArg.REDIS_HOST, "127.0.0.1") as String
    private val port: Int = channelArgs.getOrDefault(ChannelArg.REDIS_PORT, 6379) as Int
    private val db: Int = channelArgs.getOrDefault(ChannelArg.REDIS_DB, 0) as Int

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

class RedisOutputChannel<Type>(
    name: ChannelID,
    channelArgs: ChannelArgs
) : OutputChannel<Type>(name) {

    private val host: String = channelArgs.getOrDefault(ChannelArg.REDIS_HOST, "127.0.0.1") as String
    private val port: Int = channelArgs.getOrDefault(ChannelArg.REDIS_PORT, 6379) as Int
    private val db: Int = channelArgs.getOrDefault(ChannelArg.REDIS_DB, 0) as Int

    override fun connect() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun put(recordBatch: Pair<Instant?, List<Type>>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun flush() {
        // Do nothing
    }

    override fun close(timestamp: Instant?) {
        TODO("Not yet implemented")
    }
}
