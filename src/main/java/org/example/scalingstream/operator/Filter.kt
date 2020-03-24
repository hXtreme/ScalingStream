package org.example.scalingstream.operator

import org.example.scalingstream.control.channel.ChannelReadManager
import org.example.scalingstream.control.channel.ChannelWriteManager
import java.util.*

class Filter<InputType>(
    taskID: UUID,
    operatorID: String,
    channelReadManagerList: List<ChannelReadManager<InputType>>,
    channelWriteManagerList: List<ChannelWriteManager<InputType>>,
    operatorFn: (InputType) -> Boolean
) : SingleInputTask<InputType, InputType, Boolean, InputType>(
    taskID,
    operatorID,
    channelReadManagerList,
    channelWriteManagerList,
    operatorFn
) {
    override fun processBatch(batch: List<InputType>) {
        val processed = batch.filter(operatorFn)
        channelWriteManagerList.forEach { it.put(processed) }
        numProduced += processed.size
    }

    override fun processRecord(record: InputType): InputType {
        error("Unused function, should not have been called.")
    }
}
