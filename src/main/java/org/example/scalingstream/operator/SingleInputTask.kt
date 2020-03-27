package org.example.scalingstream.operator

import de.jupf.staticlog.Log
import org.example.scalingstream.control.channel.ChannelReaderManager
import org.example.scalingstream.control.channel.ChannelWriterManager
import org.example.scalingstream.extensions.*
import java.util.*

typealias SingleInputSimpleTask<InputType, OutputType> =
        SingleInputTask<InputType, InputType, OutputType, OutputType>

abstract class SingleInputTask<InputType, FnInp, FnOut, OutputType>(
    taskID: UUID,
    operatorID: String,
    channelReaderManagerList: List<ChannelReaderManager<InputType>>,
    channelWriterManagerList: List<ChannelWriterManager<OutputType>>,
    operatorFn: (FnInp) -> FnOut
) : AbstractTask<InputType, FnInp, FnOut, OutputType>(
    taskID,
    operatorID,
    channelReaderManagerList,
    channelWriterManagerList,
    operatorFn
) {
    override fun run() {
        Log.info("Running task.", name)
        while (channelReaderManagerList.any { !it.isClosed }) {
            while (channelReaderManagerList.any { it.isNotEmpty }) {
                val selectedChannelReaderManager =
                    (inputChannelManagers.take(channelReaderManagerList.size).find { it.isNotEmpty })!!
                        //?: inputChannelManagers.first()
                val (timestamp, batch) = selectedChannelReaderManager.get()

                channelWriterManagerList.forEach { it.timestamp = timestamp }
                processBatch(batch)
                numConsumed += batch.size
            }

        }

        Log.debug("Processed $numConsumed records.", name)
        Log.info("Closing buffers and quitting.", name)
        channelReaderManagerList.forEach { it.close() }
        channelWriterManagerList.forEach { it.close() }
    }
}
