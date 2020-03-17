package org.example.scalingstream.control

interface ChannelManager<Type> {
    fun connect()
    fun close()
}
