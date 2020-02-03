package org.example.scalingstream.channels

enum class ChannelArgs {
    LOCAL_QUEUE_DICT, MAX_QUEUE_LEN
}

class ChannelBuilder(
    private val channelContext: (String, Map<ChannelArgs, Any>) -> DataChannelContext,
    private val inputChannel: (String, Map<ChannelArgs, Any>) -> InputChannel,
    private val outputChannel: (String, Map<ChannelArgs, Any>) -> OutputChannel,
    private val channelArgs: Map<ChannelArgs, Any>
) {

    fun buildChannelContext(name: String): DataChannelContext {
        return channelContext(name, channelArgs)
    }

    fun buildInputChannel(name: String): InputChannel {
        return inputChannel(name, channelArgs)
    }

    fun buildOutputChannel(name: String): OutputChannel {
        return outputChannel(name, channelArgs)
    }
}


