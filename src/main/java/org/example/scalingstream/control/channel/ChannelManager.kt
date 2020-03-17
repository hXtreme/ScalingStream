package org.example.scalingstream.control.channel

interface ChannelManager<Type> {
    fun connect()
    fun close()
}
