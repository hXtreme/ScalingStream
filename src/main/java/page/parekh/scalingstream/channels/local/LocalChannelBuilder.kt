package page.parekh.scalingstream.channels.local

import page.parekh.scalingstream.channels.*


object LocalChannelConstants {
    const val TYPE = "LOCAL"
}

open class LocalChannelBuilder(
    override val channelArgs: ChannelArgs
) : ChannelBuilder {
    override val type: String = LocalChannelConstants.TYPE

    override fun <Type> buildChannel(id: ChannelID): Channel<Type> {
        return LocalChannel(id, channelArgs)
    }
}
