package page.parekh.scalingstream.channels.redis

import page.parekh.scalingstream.channels.*


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
        return RedisChannel(id, channelArgs)
    }
}
