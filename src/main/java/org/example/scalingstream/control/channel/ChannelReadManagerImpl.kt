package org.example.scalingstream.control.channel

class ChannelReadManagerImpl<Type> : ChannelReadManager<Type>() {
    override var closedAndEmpty: Boolean
        get() = false //TODO("Not yet implemented")
        set(value) {}

    override fun close() {
        // TODO("Not yet implemented")

    }
}