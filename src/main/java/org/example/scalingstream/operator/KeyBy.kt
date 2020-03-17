package org.example.scalingstream.operator

import org.example.scalingstream.control.InputChannelManager
import org.example.scalingstream.control.OutputChannelManager

class KeyBy<InputType, KeyType>(
    taskID: Int,
    operatorID: String,
    inputChannelManagers: List<InputChannelManager<InputType>>,
    outputChannelManagers: List<OutputChannelManager<Pair<KeyType, InputType>>>,
    operatorFn: (InputType) -> KeyType
) : SingleInputTask<InputType, InputType, KeyType, Pair<KeyType, InputType>>(
    taskID,
    operatorID,
    inputChannelManagers,
    outputChannelManagers,
    operatorFn
) {
    override fun processRecord(record: InputType): Pair<KeyType, InputType> {
        return Pair(operatorFn(record), record)
    }
}
