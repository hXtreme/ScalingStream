package org.example.scalingstream.channels

abstract class DataChannelContext<Type> internal constructor(var name: String?) {
    var channelArgs: Map<ChannelArg, Any>? = null

    internal constructor(name: String?, channelArgs: Map<ChannelArg, Any>?) : this(name) {
        this.channelArgs = channelArgs
    }

    abstract fun destroy()

}