package org.example.scalingstream.channels

abstract class InputChannel<Type>(override val id: ChannelID) : ChannelIO {
    abstract fun peek(): Record<Type>?

    abstract fun get(): Record<Type>

    abstract fun close()
}
