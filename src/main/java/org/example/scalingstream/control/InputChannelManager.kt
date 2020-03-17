package org.example.scalingstream.control

import java.time.Instant

interface InputChannelManager<Type> : ChannelManager<Type> {
    fun peek(): Pair<Instant?, List<Type>?>
    fun get(): Pair<Instant?, List<Type>>

    var closedAndEmpty: Boolean

}