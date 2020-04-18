package page.parekh.scalingstream.channels.redis

import page.parekh.scalingstream.channels.*
import page.parekh.scalingstream.extensions.*
import redis.clients.jedis.Jedis

class RedisChannelReader<Type>(
    id: ChannelID,
    channelArgs: ChannelArgs
) : ChannelReader<Type>(id) {

    private val host: String = channelArgs.getOrDefault(ChannelArg.REDIS_HOST, "127.0.0.1") as String
    private val port: Int = channelArgs.getOrDefault(ChannelArg.REDIS_PORT, 6379) as Int
    private val db: Int = channelArgs.getOrDefault(ChannelArg.REDIS_DB, 0) as Int

    private val jedis = Jedis(host, port)

    override val isClosed: Boolean
        get() = jedis.get("$id-status") != "open"
    override val isNotClosed: Boolean
        get() = jedis.get("$id-status") == "open"
    override val isNotEmpty: Boolean
        get() = jedis.llen("$id") > 0
    override val isEmpty: Boolean
        get() = jedis.llen("$id") == 0L

    override fun peek(): Record<Type>? {
        if (isEmpty) return null
        return deserializeFromString(jedis.lrange("$id", 0, 0)[0])
    }

    override fun get(): Record<Type> {
        if (isClosed && isEmpty) error("Can't read from an empty closed channel.")
        return deserializeFromString(jedis.blpop(0, "$id")[1])
    }

    override fun close() {
        jedis.close()
    }

    override fun connect() {}
}
