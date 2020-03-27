package org.example.scalingstream.channels

abstract class AbstractChannel<Type>(
    override val id: ChannelID,
    val channelArgs: ChannelArgs
) : Channel<Type> {

}
