package org.example.scalingstream.operator

import org.example.scalingstream.control.channel.ChannelReaderManager
import org.example.scalingstream.control.channel.ChannelWriterManager
import java.util.*

class Filter<InputType>(
    taskID: UUID,
    operatorID: String,
    channelReaderManagerList: List<ChannelReaderManager<InputType>>,
    channelWriterManagerList: List<ChannelWriterManager<InputType>>,
    operatorFn: (InputType) -> Boolean
) : SingleInputTask<InputType, InputType, Boolean, InputType>(
    taskID,
    operatorID,
    channelReaderManagerList,
    channelWriterManagerList,
    operatorFn
) {
    override fun processBatch(batch: List<InputType>) {
        val processed = batch.filter(operatorFn)
        channelWriterManagerList.forEach { it.put(processed) }
        numProduced += processed.size
    }

    override fun processRecord(record: InputType): InputType {
        error("Unused function, should not have been called.")
    }
}
