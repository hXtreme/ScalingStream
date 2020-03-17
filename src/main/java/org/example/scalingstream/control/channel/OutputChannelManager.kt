package org.example.scalingstream.control

import java.time.Instant

interface OutputChannelManager<Type> : ChannelManager<Type> {
    fun put(batch: List<Type>)

    var timestamp: Instant?
}