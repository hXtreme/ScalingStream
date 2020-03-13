package org.example.scalingstream.channels

import java.time.Instant

abstract class InputChannel<Type>(name: String?) : DataChannel(name) {
    abstract fun peek(): Pair<Instant?, List<Type>?>?

    abstract fun get(): Pair<Instant?, List<Type>?>
}