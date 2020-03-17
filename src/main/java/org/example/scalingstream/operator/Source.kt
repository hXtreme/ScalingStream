package org.example.scalingstream.operator

import de.jupf.staticlog.Log
import org.example.scalingstream.CONSTANTS
import org.example.scalingstream.control.OutputChannelManager
import java.time.Instant


class Source<OutputType>(
    taskID: Int,
    operatorID: String,
    private val batchSize: Int,
    outputChannelManagerList: List<OutputChannelManager<OutputType>>,
    operatorFn: (Unit) -> OutputType
) : SimpleTask<Unit, OutputType>(taskID, operatorID, emptyList(), outputChannelManagerList, operatorFn) {

    private var numBatches = 0

    override fun run() {
        Log.info("Running source task.", tag)
        outputChannelManagerList.forEach { it.connect() }

        val batches = generateSequence { operatorFn(Unit) }.chunked(batchSize)

        batches.forEach { batch ->
            if (numBatches % CONSTANTS.TIMESTAMP_INTERVAL == 0) {
                val timestamp = Instant.now()
                outputChannelManagerList.forEach { it.timestamp = timestamp }
            }
            numBatches++

            outputChannelManagerList.forEach { it.put(batch) }
            numProduced += batch.size
        }

        Log.debug("Source generated $numProduced records.", tag)
        Log.info("Closing buffers and quitting.", tag)
        outputChannelManagerList.forEach { it.close() }
    }

    override fun processBatch(batch: List<Unit>) = Unit
    override fun processRecord(record: Unit): OutputType {
        error("Unused function, should not have been called.")
    }
}
