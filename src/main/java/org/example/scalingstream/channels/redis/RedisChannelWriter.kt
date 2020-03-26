package org.example.scalingstream.channels.redis

import org.example.scalingstream.channels.*
import java.time.Instant

class RedisChannelWriter<Type>(
    name: ChannelID,
    channelArgs: ChannelArgs
) : ChannelWriter<Type>(name) {

    private val host: String = channelArgs.getOrDefault(ChannelArg.REDIS_HOST, "127.0.0.1") as String
    private val port: Int = channelArgs.getOrDefault(ChannelArg.REDIS_PORT, 6379) as Int
    private val db: Int = channelArgs.getOrDefault(ChannelArg.REDIS_DB, 0) as Int

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