package page.parekh.scalingstream.control.channel

class ChannelReaderManagerImpl<Type> : ChannelReaderManager<Type>() {
    override val isNotClosed: Boolean
        get() {
            return channelReaderMap.any { (_, v) -> !v.isClosed }
        }
    override val isClosed: Boolean
        get() {
            return !channelReaderMap.any { (_, v) -> !v.isClosed }
        }
    override val isEmpty: Boolean
        get() {
            return !channelReaderMap.any { (_, v) -> v.isNotEmpty }
        }
    override val isNotEmpty: Boolean
        get() {
            return channelReaderMap.any { (_, v) -> v.isNotEmpty }
        }

    override fun close() {
        // TODO("Not yet implemented")

    }
}
