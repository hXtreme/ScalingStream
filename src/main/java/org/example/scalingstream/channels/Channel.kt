package org.example.scalingstream.channels

import java.util.*

typealias ChannelID = Pair<UUID, UUID>

interface Channel<Type> {
    val id: ChannelID

    fun getChannelReader(): InputChannel<Type>

    fun getChannelWriter() : OutputChannel<Type>

    fun destroy()
}