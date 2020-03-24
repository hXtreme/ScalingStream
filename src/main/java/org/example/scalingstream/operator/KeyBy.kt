package org.example.scalingstream.operator

import org.example.scalingstream.control.channel.ChannelReadManager
import org.example.scalingstream.control.channel.ChannelWriteManager
import java.util.*

class KeyBy<InputType, KeyType>(
    taskID: UUID,
    operatorID: String,
    channelReadManagerList: List<ChannelReadManager<InputType>>,
    channelWriteManagerList: List<ChannelWriteManager<Pair<KeyType, InputType>>>,
    operatorFn: (InputType) -> KeyType
) : SingleInputTask<InputType, InputType, KeyType, Pair<KeyType, InputType>>(
    taskID,
    operatorID,
    channelReadManagerList,
    channelWriteManagerList,
    operatorFn
) {
    override fun processRecord(record: InputType): Pair<KeyType, InputType> {
        return Pair(operatorFn(record), record)
    }
}
