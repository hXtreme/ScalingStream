package org.example.scalingstream.channels

class JiffyChannel<Type>(id: ChannelID, channelArgs: ChannelArgs) : AbstractChannel<Type>(id, channelArgs) {
    override val type: String = "JIFFY"

    init {
        TODO("Not yet implemented")
    }

    override fun getChannelReader(): InputChannel<Type> {
        return JiffyInputChannel(id, channelArgs)
    }

    override fun getChannelWriter(): OutputChannel<Type> {
        return JiffyOutputChannel(id, channelArgs)
    }

    override fun destroy() {
        TODO("Not yet implemented")
    }
}