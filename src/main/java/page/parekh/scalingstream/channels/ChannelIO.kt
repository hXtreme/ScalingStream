package page.parekh.scalingstream.channels

import java.io.Serializable
import java.time.Instant

data class Record<Type>(val timestamp: Instant?, val batch: List<Type>) : Serializable

interface ChannelIO {
    val id: ChannelID

    fun connect()
}
