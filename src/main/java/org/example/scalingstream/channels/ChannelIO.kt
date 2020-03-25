package org.example.scalingstream.channels

import java.time.Instant

data class Record<Type>(val timestamp: Instant?, val batch: List<Type>)

interface ChannelIO {
    val id: ChannelID

    fun connect()
}