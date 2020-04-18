package page.parekh.scalingstream.channels.redis

import page.parekh.scalingstream.channels.*
import redis.clients.jedis.Jedis

class RedisChannel<Type>(
    id: ChannelID,
    channelArgs: ChannelArgs,
    jedis: Jedis
) : AbstractChannel<Type>(id, channelArgs) {
    override val type: String = "REDIS"

    init {
        jedis.set("$id-status", "open")
    }

    override fun getChannelReader(): ChannelReader<Type> {
        return RedisChannelReader(id, channelArgs)
    }

    override fun getChannelWriter(): ChannelWriter<Type> {
        return RedisChannelWriter(id, channelArgs)
    }

    override fun destroy() {
        error("Should not have been invoked.")
    }
}
