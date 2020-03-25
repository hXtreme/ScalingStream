package org.example.scalingstream.channels

import java.time.Instant


abstract class OutputChannel<Type>(override val id: ChannelID) : ChannelIO {
    abstract fun put(recordBatch: Record<Type>)
    abstract fun flush()
    abstract fun close(timestamp: Instant?)
}
