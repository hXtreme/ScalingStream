package org.example.scalingstream.channels.redis

import org.example.scalingstream.channels.*

class RedisChannel<Type>(id: ChannelID, channelArgs: ChannelArgs) : AbstractChannel<Type>(id, channelArgs) {
    override val type: String= "REDIS"

    init {
        TODO("Not yet implemented")
    }

    override fun getChannelReader(): InputChannel<Type> {
        return RedisInputChannel(id, channelArgs)
    }

    override fun getChannelWriter(): OutputChannel<Type> {
        return RedisOutputChannel(id, channelArgs)
    }

    override fun destroy() {
        TODO("Not yet implemented")
    }
}