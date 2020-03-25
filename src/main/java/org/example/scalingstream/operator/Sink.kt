package org.example.scalingstream.operator

import de.jupf.staticlog.Log
import org.example.scalingstream.control.channel.ChannelReadManager
import org.example.scalingstream.control.channel.ChannelWriteManager
import org.example.scalingstream.extensions.*
import java.time.Duration
import java.time.Instant
import java.util.*

class Sink<InputType>(
    taskID: UUID,
    operatorID: String,
    channelReadManagerList: List<ChannelReadManager<InputType>>,
    channelWriteManagerList: List<ChannelWriteManager<Unit>>,
    operatorFn: (InputType) -> Unit
) : AbstractTask<InputType, InputType, Unit, Unit>(
    taskID,
    operatorID,
    channelReadManagerList,
    emptyList(),
    operatorFn
) {

    override fun run() {
        Log.info("Running sink task", toString())

        while (channelReadManagerList.any { !it.closedAndEmpty }) {
            val (timestamp, batch) =
                (inputChannelManagers.take(channelReadManagerList.size).find { it.isReady() != null }?.get())
                    ?: inputChannelManagers.first().get()

            timestamp?.let { Log.debug("Latency: ${Duration.between(timestamp, Instant.now())}", toString()) }
            processBatch(batch)
            numConsumed += batch.size
        }
        Log.debug("Processed $numConsumed records.", toString())
        Log.info("Closing buffers and quitting.", toString())
        channelReadManagerList.forEach { it.close() }
    }

    override fun processBatch(batch: List<InputType>) {
        batch.forEach { operatorFn(it) }
    }

    override fun processRecord(record: InputType) {
        error("Unused function, should not have been called.")
    }
}
