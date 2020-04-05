package org.example.scalingstream.channels

import java.util.*

data class ChannelID(val src: UUID, val dst: UUID)

interface Channel<Type> {
    val id: ChannelID
    val type: String

    fun getChannelReader(): ChannelReader<Type>

    fun getChannelWriter(): ChannelWriter<Type>

    fun destroy()
}
