package org.example.scalingstream.operator

import org.example.scalingstream.control.channel.ChannelReaderManager
import org.example.scalingstream.control.channel.ChannelWriterManager
import java.util.*

class Map<InputType, OutputType>(
    taskID: UUID,
    operatorID: String,
    channelReaderManagerList: List<ChannelReaderManager<InputType>>,
    channelWriterManagerList: List<ChannelWriterManager<OutputType>>,
    operatorFn: (InputType) -> OutputType
) : SingleInputSimpleTask<InputType, OutputType>(
    taskID,
    operatorID,
    channelReaderManagerList,
    channelWriterManagerList,
    operatorFn
) {
    override fun processRecord(record: InputType): OutputType {
        return operatorFn(record)
    }
}
