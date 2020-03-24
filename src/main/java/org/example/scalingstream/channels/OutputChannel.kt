package org.example.scalingstream.channels

import java.time.Instant
import java.util.*


abstract class OutputChannel<Type>(id: ChannelID) : DataChannel(id) {
    abstract fun put(recordBatch: Record<Type>)
    abstract fun flush()
    abstract fun close(timestamp: Instant?)
}
