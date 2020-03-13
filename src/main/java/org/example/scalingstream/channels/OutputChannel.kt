package org.example.scalingstream.channels

import java.time.Instant


abstract class OutputChannel<Type>(name: String?) : DataChannel(name) {
    abstract fun put(recordBatch: Pair<Instant?, List<Type>?>)
    abstract fun flush()
}
