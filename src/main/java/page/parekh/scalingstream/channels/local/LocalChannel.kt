package page.parekh.scalingstream.channels.local

import page.parekh.scalingstream.channels.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

class LocalChannel<Type>(id: ChannelID, channelArgs: ChannelArgs) : AbstractChannel<Type>(id, channelArgs) {
    override val type: String = "LOCAL"
    private val maxQueueLength: Int = channelArgs.getOrDefault(ChannelArg.MAX_QUEUE_LEN, Int.Companion.MAX_VALUE) as Int
    private val queueDict =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as MutableMap<ChannelID, CloseableLinkedBlockingQueue<Record<Type>>>

    init {
        queueDict.putIfAbsent(id, CloseableLinkedBlockingQueue(maxQueueLength))
    }

    override fun destroy() {
        queueDict.remove(id)
    }

    override fun getChannelReader(): ChannelReader<Type> {
        return LocalChannelReader(id, channelArgs)
    }

    override fun getChannelWriter(): ChannelWriter<Type> {
        return LocalChannelWriter(id, channelArgs)
    }
}
