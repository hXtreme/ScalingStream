package page.parekh.scalingstream.channels.redis

import page.parekh.scalingstream.channels.*
import page.parekh.scalingstream.extensions.*
import redis.clients.jedis.Jedis
import java.time.Instant

class RedisChannelWriter<Type>(
    name: ChannelID,
    channelArgs: ChannelArgs
) : ChannelWriter<Type>(name) {

    private val host: String = channelArgs.getOrDefault(ChannelArg.REDIS_HOST, "127.0.0.1") as String
    private val port: Int = channelArgs.getOrDefault(ChannelArg.REDIS_PORT, 6379) as Int
    private val db: Int = channelArgs.getOrDefault(ChannelArg.REDIS_DB, 0) as Int

    private val jedis = Jedis(host, port)

    override fun put(recordBatch: Record<Type>) {
        val serializedBatch = recordBatch.serializeToString()
        jedis.rpush("$id", serializedBatch)
    }

    override fun close(timestamp: Instant?) {
        jedis.set("$id-status", "closed")
        jedis.close()
    }

    override fun flush() {}

    override fun connect() {}
}
