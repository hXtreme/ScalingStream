package org.example.scalingstream.operator

import org.example.scalingstream.control.channel.InputChannelManager
import org.example.scalingstream.control.channel.OutputChannelManager

class Filter<InputType>(
    taskID: Int,
    operatorID: String,
    inputChannelManagers: List<InputChannelManager<InputType>>,
    outputChannelManagers: List<OutputChannelManager<InputType>>,
    operatorFn: (InputType) -> Boolean
) : SingleInputTask<InputType, InputType, Boolean, InputType>(
    taskID,
    operatorID,
    inputChannelManagers,
    outputChannelManagers,
    operatorFn
) {
    override fun processBatch(batch: List<InputType>) {
        val processed = batch.filter(operatorFn)
        outputChannelManagerList.forEach { it.put(processed) }
        numProduced += processed.size
    }

    override fun processRecord(record: InputType): InputType {
        error("Unused function, should not have been called.")
    }
}
