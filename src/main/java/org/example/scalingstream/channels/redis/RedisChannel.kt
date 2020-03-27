package org.example.scalingstream.channels.redis

import org.example.scalingstream.channels.*

class RedisChannel<Type>(id: ChannelID, channelArgs: ChannelArgs) : AbstractChannel<Type>(id, channelArgs) {
    override val type: String= "REDIS"

    init {
        TODO("Not yet implemented")
    }

    override fun getChannelReader(): ChannelReader<Type> {
        return RedisChannelReader(id, channelArgs)
    }

    override fun getChannelWriter(): ChannelWriter<Type> {
        return RedisChannelWriter(id, channelArgs)
    }

    override fun destroy() {
        TODO("Not yet implemented")
    }
}
