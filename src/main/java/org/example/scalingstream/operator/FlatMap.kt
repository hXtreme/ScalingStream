package org.example.scalingstream.operator

import org.example.scalingstream.control.channel.ChannelReadManager
import org.example.scalingstream.control.channel.ChannelWriteManager
import java.util.*

class FlatMap<InputType, OutputType>(
    taskID: UUID,
    operatorID: String,
    channelReadManagerList: List<ChannelReadManager<InputType>>,
    channelWriteManagerList: List<ChannelWriteManager<OutputType>>,
    operatorFn: (InputType) -> Iterable<OutputType>
) : SingleInputTask<InputType, InputType, Iterable<OutputType>, OutputType>(
    taskID,
    operatorID,
    channelReadManagerList,
    channelWriteManagerList,
    operatorFn
) {
    override fun processBatch(batch: List<InputType>) {
        val processed = batch.flatMap { it -> operatorFn(it) }
        channelWriteManagerList.forEach { it.put(processed) }
        numProduced += processed.size
    }

    override fun processRecord(record: InputType): OutputType {
        error("Unused function, should not have been called.")
    }
}
