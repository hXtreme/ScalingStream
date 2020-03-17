package org.example.scalingstream.operator

import de.jupf.staticlog.Log
import org.example.scalingstream.control.channel.InputChannelManager
import java.time.Duration
import java.time.Instant

class Sink<InputType>(
    taskID: Int,
    operatorID: String,
    inputChannelManagerList: List<InputChannelManager<InputType>>,
    operatorFn: (InputType) -> Unit
) : SimpleTask<InputType, Unit>(taskID, operatorID, inputChannelManagerList, emptyList(), operatorFn) {

    override fun run() {
        Log.info("Running sink task", tag)
        inputChannelManagerList.forEach { it.connect() }

        while (inputChannelManagerList.any { !it.closedAndEmpty }) {
            val (timestamp, batch) =
                (inputChannelManagers.take(inputChannelManagerList.size).find { it.peek() != null }?.get())
                    ?: inputChannelManagers.first().get()

            timestamp?.let { Log.debug("Latency: ${Duration.between(timestamp, Instant.now())}", tag) }
            processBatch(batch)
            numConsumed += batch.size
        }
        Log.debug("Processed $numConsumed records.", tag)
        Log.info("Closing buffers and quitting.", tag)
        inputChannelManagerList.forEach { it.close() }
    }

    override fun processBatch(batch: List<InputType>) {
        batch.forEach { operatorFn(it) }
    }

    override fun processRecord(record: InputType) {
        error("Unused function, should not have been called.")
    }
}
