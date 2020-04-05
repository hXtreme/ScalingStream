package page.parekh.scalingstream.operator

import page.parekh.scalingstream.control.channel.ChannelReaderManager
import page.parekh.scalingstream.control.channel.ChannelWriterManager
import java.util.*

class KeyBy<InputType, KeyType>(
    taskID: UUID,
    operatorID: String,
    channelReaderManagerList: List<ChannelReaderManager<InputType>>,
    channelWriterManagerList: List<ChannelWriterManager<Pair<KeyType, InputType>>>,
    operatorFn: (InputType) -> KeyType
) : SingleInputTask<InputType, InputType, KeyType, Pair<KeyType, InputType>>(
    taskID,
    operatorID,
    channelReaderManagerList,
    channelWriterManagerList,
    operatorFn
) {
    override fun processRecord(record: InputType): Pair<KeyType, InputType> {
        return Pair(operatorFn(record), record)
    }
}
