package org.example.scalingstream.operator

import org.example.scalingstream.control.InputChannelManager
import org.example.scalingstream.control.OutputChannelManager

class Map<InputType, OutputType>(
    taskID: Int,
    operatorID: String,
    inputChannelManagers: List<InputChannelManager<InputType>>,
    outputChannelManagers: List<OutputChannelManager<OutputType>>,
    operatorFn: (InputType) -> OutputType
) : SingleInputSimpleTask<InputType, OutputType>(
    taskID,
    operatorID,
    inputChannelManagers,
    outputChannelManagers,
    operatorFn
) {
    override fun processRecord(record: InputType): OutputType {
        return operatorFn(record)
    }
}
