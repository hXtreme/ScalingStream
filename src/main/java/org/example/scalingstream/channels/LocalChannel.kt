package org.example.scalingstream.channels

import java.util.*
import java.util.concurrent.LinkedBlockingQueue

class LocalChannel<Type>(id: ChannelID, channelArgs: ChannelArgs) : AbstractChannel<Type>(id, channelArgs) {
    override val type: String = "LOCAL"
    private val maxQueueLength: Int = channelArgs.getOrDefault(ChannelArg.MAX_QUEUE_LEN, Int.Companion.MAX_VALUE) as Int
    private val queueDict = channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as MutableMap<ChannelID, Queue<Record<Type>?>>

    init {
        queueDict.putIfAbsent(id, LinkedBlockingQueue(maxQueueLength))
    }

    override fun destroy() {
        queueDict.remove(id)
    }

    override fun getChannelReader(): InputChannel<Type> {
        return LocalInputChannel(id, channelArgs)
    }

    override fun getChannelWriter(): OutputChannel<Type> {
        return LocalOutputChannel(id, channelArgs)
    }
}