package org.example.scalingstream.operator

import org.example.scalingstream.control.InputChannelManager
import org.example.scalingstream.control.OutputChannelManager

class FlatMap<InputType, OutputType>(
    taskID: Int,
    operatorID: String,
    inputChannelManagers: List<InputChannelManager<InputType>>,
    outputChannelManagers: List<OutputChannelManager<OutputType>>,
    operatorFn: (InputType) -> Iterable<OutputType>
) : SingleInputTask<InputType, InputType, Iterable<OutputType>, OutputType>(
    taskID,
    operatorID,
    inputChannelManagers,
    outputChannelManagers,
    operatorFn
) {
    override fun processBatch(batch: List<InputType>) {
        val processed = batch.flatMap { it -> operatorFn(it) }
        outputChannelManagerList.forEach { it.put(processed) }
        numProduced += processed.size
    }

    override fun processRecord(record: InputType): OutputType {
        error("Unused function, should not have been called.")
    }
}
