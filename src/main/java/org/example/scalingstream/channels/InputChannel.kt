package org.example.scalingstream.channels

abstract class InputChannel<Type>(id: ChannelID) : DataChannel(id) {
    abstract fun peek(): Record<Type>?

    abstract fun get(): Record<Type>
    fun close() {
        TODO("Not yet implemented")
    }
}
