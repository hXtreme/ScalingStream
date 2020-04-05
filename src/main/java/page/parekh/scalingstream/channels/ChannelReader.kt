package org.example.scalingstream.channels

abstract class ChannelReader<Type>(override val id: ChannelID) : ChannelIO {
    abstract val isClosed: Boolean
    abstract val isNotClosed: Boolean
    abstract val isNotEmpty: Boolean
    abstract val isEmpty: Boolean

    abstract fun peek(): Record<Type>?

    abstract fun get(): Record<Type>

    abstract fun close()
}
