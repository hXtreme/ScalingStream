package page.parekh.scalingstream.channels.jiffy

import page.parekh.scalingstream.channels.*


object JiffyChannelConstants {
    const val TYPE = "JIFFY"
}

class JiffyChannelBuilder(
    override val channelArgs: ChannelArgs
) : ChannelBuilder {
    override val type: String = JiffyChannelConstants.TYPE

    init {
        channelArgs.putIfAbsent(ChannelArg.JIFFY_HOST, "127.0.0.1")
        channelArgs.putIfAbsent(ChannelArg.JIFFY_SERVICE_PORT, 9090)
        channelArgs.putIfAbsent(ChannelArg.JIFFY_LEASE_PORT, 9091)
    }

    override fun <Type> buildChannel(id: ChannelID): Channel<Type> {
        return JiffyChannel(id, channelArgs)
    }
}
