package page.parekh.scalingstream.channels.jiffy

import page.parekh.scalingstream.channels.*

class JiffyChannel<Type>(id: ChannelID, channelArgs: ChannelArgs) : AbstractChannel<Type>(id, channelArgs) {
    override val type: String = "JIFFY"

    init {
        TODO("Not yet implemented")
    }

    override fun getChannelReader(): ChannelReader<Type> {
        return JiffyChannelReader(id, channelArgs)
    }

    override fun getChannelWriter(): ChannelWriter<Type> {
        return JiffyChannelWriter(id, channelArgs)
    }

    override fun destroy() {
        TODO("Not yet implemented")
    }
}
