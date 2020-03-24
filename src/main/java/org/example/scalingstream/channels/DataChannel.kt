package org.example.scalingstream.channels

import java.time.Instant

typealias Record<Type> = Pair<Instant?, List<Type>>

abstract class DataChannel(var id: ChannelID) {

    abstract fun connect()

}