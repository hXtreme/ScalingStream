package org.example.scalingstream.channels


enum class ChannelArg {
    LOCAL_QUEUE_DICT, MAX_QUEUE_LEN,
    JIFFY_HOST, JIFFY_SERVICE_PORT, JIFFY_LEASE_PORT,
    REDIS_HOST, REDIS_PORT, REDIS_DB
}

typealias ChannelArgs = HashMap<ChannelArg, Any>

interface ChannelBuilder {
    val type: String
    val channelArgs: ChannelArgs

    fun <Type> buildChannel(id: ChannelID): Channel<Type>
}
