package page.parekh.scalingstream.channels.redis

import page.parekh.scalingstream.channels.*
import redis.clients.jedis.JedisPool


object RedisChannelConstants {
    const val TYPE = "REDIS"
}

class RedisChannelBuilder(
    override val channelArgs: ChannelArgs
) : ChannelBuilder {
    override val type: String = RedisChannelConstants.TYPE
    private lateinit var jedisPool: JedisPool

    init {
        val host = channelArgs.getOrPut(ChannelArg.REDIS_HOST) {"127.0.0.1"} as String
        val port = channelArgs.getOrPut(ChannelArg.REDIS_PORT) {6379} as Int
        /* TODO: Not sure what this db is for, hopefully I'll figure it out as I go. */
        val db = channelArgs.getOrPut(ChannelArg.REDIS_DB) {0} as Int
        jedisPool = JedisPool(host, port)
    }

    override fun <Type> buildChannel(id: ChannelID): Channel<Type> {
        val jedis = jedisPool.resource
        jedis.use {
            return RedisChannel(id, channelArgs, jedis)
        }
    }
}
