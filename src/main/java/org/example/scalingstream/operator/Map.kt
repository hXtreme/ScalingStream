package org.example.scalingstream.operator

import org.example.scalingstream.control.channel.ChannelReadManager
import org.example.scalingstream.control.channel.ChannelWriteManager
import java.util.*

class Map<InputType, OutputType>(
    taskID: UUID,
    operatorID: String,
    channelReadManagerList: List<ChannelReadManager<InputType>>,
    channelWriteManagerList: List<ChannelWriteManager<OutputType>>,
    operatorFn: (InputType) -> OutputType
) : SingleInputSimpleTask<InputType, OutputType>(
    taskID,
    operatorID,
    channelReadManagerList,
    channelWriteManagerList,
    operatorFn
) {
    override fun processRecord(record: InputType): OutputType {
        return operatorFn(record)
    }
}
