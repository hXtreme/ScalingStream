package org.example.scalingstream.channels

import java.time.Instant


object RedisChannelConstants {
    const val TYPE = "REDIS"
}


class RedisChannelBuilder(
    override val name: String,
    override val channelArgs: ChannelArgs
) : ChannelBuilder {
    override val type: String = RedisChannelConstants.TYPE

    private val host: String = channelArgs.getOrDefault(ChannelArg.REDIS_HOST, "127.0.0.1") as String
    private val port: Int = channelArgs.getOrDefault(ChannelArg.REDIS_PORT, 6379) as Int
    private val db: Int = channelArgs.getOrDefault(ChannelArg.REDIS_DB, 0) as Int

    override fun <Type> buildChannelContext(name: String): DataChannelContext {
        return RedisChannelContext(name, host, port, db)
    }

    override fun <Type> buildInputChannel(name: String): InputChannel<Type> {
        return RedisInputChannel(name, host, port, db)
    }

    override fun <Type> buildOutputChannel(name: String): OutputChannel<Type> {
        return RedisOutputChannel(name, host, port, db)
    }
}

class RedisChannelContext(
    name: String,
    private val host: String = "127.0.0.1",
    private val port: Int = 6379,
    private val db: Int = 0
) : DataChannelContext(name) {

    init {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun destroy() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

class RedisInputChannel<Type>(
    name: String,
    private val host: String = "127.0.0.1",
    private val port: Int = 6379,
    private val db: Int = 0
) : InputChannel<Type>(name) {

    override fun connect() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun get(): Pair<Instant?, List<Type>?> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

class RedisOutputChannel<Type>(
    name: String,
    private val host: String = "127.0.0.1",
    private val port: Int = 6379,
    private val db: Int = 0
) : OutputChannel<Type>(name) {

    override fun connect() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun put(record: Pair<Instant?, List<Type>?>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun flush() {
        // Do nothing
    }
}
