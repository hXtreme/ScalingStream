package org.example.scalingstream.operator

import de.jupf.staticlog.Log
import org.example.scalingstream.control.channel.ChannelReadManager
import org.example.scalingstream.control.channel.ChannelWriteManager
import org.example.scalingstream.extensions.*
import java.util.*

typealias SingleInputSimpleTask<InputType, OutputType> =
        SingleInputTask<InputType, InputType, OutputType, OutputType>

abstract class SingleInputTask<InputType, FnInp, FnOut, OutputType>(
    taskID: UUID,
    operatorID: String,
    channelReadManagerList: List<ChannelReadManager<InputType>>,
    channelWriteManagerList: List<ChannelWriteManager<OutputType>>,
    operatorFn: (FnInp) -> FnOut
) : AbstractTask<InputType, FnInp, FnOut, OutputType>(
    taskID,
    operatorID,
    channelReadManagerList,
    channelWriteManagerList,
    operatorFn
) {
    override fun run() {
        Log.info("Running task.", name)
        while (channelReadManagerList.any { !it.closedAndEmpty }) {
            val (timestamp, batch) = (inputChannelManagers.take(channelReadManagerList.size)
                .find { it.isReady() } ?: inputChannelManagers.first())
                .get()

            channelWriteManagerList.forEach { it.timestamp = timestamp }
            processBatch(batch)
            numConsumed += batch.size
        }

        Log.debug("Processed $numConsumed records.", name)
        Log.info("Closing buffers and quitting.", name)
        channelReadManagerList.forEach { it.close() }
        channelWriteManagerList.forEach { it.close() }
    }
}
