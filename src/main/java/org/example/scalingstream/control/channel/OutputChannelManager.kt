package org.example.scalingstream.control.channel

import java.time.Instant

interface OutputChannelManager<Type> : ChannelManager<Type> {
    fun put(batch: List<Type>)

    var timestamp: Instant?
}
