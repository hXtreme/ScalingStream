package org.example.scalingstream.channels

import java.time.Instant


abstract class OutputChannel<Type>(name: String?) : DataChannel(name) {
    abstract fun put(record: Pair<Instant?, List<Type>?>)
    abstract fun flush()
}