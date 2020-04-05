package page.parekh.scalingstream.channels

abstract class AbstractChannel<Type>(
    override val id: ChannelID,
    val channelArgs: ChannelArgs
) : Channel<Type> {

}
