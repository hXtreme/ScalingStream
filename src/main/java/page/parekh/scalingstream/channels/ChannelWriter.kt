package page.parekh.scalingstream.channels

import java.time.Instant


abstract class ChannelWriter<Type>(override val id: ChannelID) : ChannelIO {
    abstract fun put(recordBatch: Record<Type>)
    abstract fun flush()
    abstract fun close(timestamp: Instant?)
}
