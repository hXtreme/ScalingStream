package org.example.scalingstream.operator

import de.jupf.staticlog.Log
import org.example.scalingstream.control.channel.InputChannelManager
import org.example.scalingstream.control.channel.OutputChannelManager

typealias SingleInputSimpleTask<InputType, OutputType> =
        SingleInputTask<InputType, InputType, OutputType, OutputType>

abstract class SingleInputTask<InputType, FnInp, FnOut, OutputType>(
    taskID: Int,
    operatorID: String,
    inputChannelManagers: List<InputChannelManager<InputType>>,
    outputChannelManagers: List<OutputChannelManager<OutputType>>,
    operatorFn: (FnInp) -> FnOut
) : Task<InputType, FnInp, FnOut, OutputType>(
    taskID,
    operatorID,
    inputChannelManagers,
    outputChannelManagers,
    operatorFn
) {
    override fun run() {
        Log.info("Running task.", tag)
        inputChannelManagerList.forEach { it.connect() }
        outputChannelManagerList.forEach { it.connect() }

        while (inputChannelManagerList.any { !it.closedAndEmpty }) {
            val (timestamp, batch) =
                (inputChannelManagers.take(inputChannelManagerList.size).find { it.peek() != null }?.get())
                    ?: inputChannelManagers.first().get()

            outputChannelManagerList.forEach { it.timestamp = timestamp }
            processBatch(batch)
            numConsumed += batch.size
        }

        Log.debug("Processed $numConsumed records.", tag)
        Log.info("Closing buffers and quitting.", tag)
        inputChannelManagerList.forEach { it.close() }
        outputChannelManagerList.forEach { it.close() }
    }
}
