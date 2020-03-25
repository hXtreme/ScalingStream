package org.example.scalingstream.operator

import de.jupf.staticlog.Log
import org.example.scalingstream.CONSTANTS
import org.example.scalingstream.control.channel.ChannelReadManager
import org.example.scalingstream.control.channel.ChannelWriteManager
import java.time.Instant
import java.util.*


class Source<OutputType : Any>(
    taskID: UUID,
    operatorID: String,
    channelReadManagerList: List<ChannelReadManager<Unit>>,
    channelWriteManagerList: List<ChannelWriteManager<OutputType>>,
    operatorFn: (Unit) -> OutputType?
) : AbstractTask<Unit, Unit, OutputType?, OutputType>(
    taskID,
    operatorID,
    emptyList(),
    channelWriteManagerList,
    operatorFn
) {
    private var batchSize = 1

    private var numBatches = 0

    override fun run() {
        Log.info("Running source task.", toString())
        val batches = generateSequence { operatorFn(Unit) }.chunked(batchSize)

        batches.forEach { batch ->
            if (numBatches % CONSTANTS.TIMESTAMP_INTERVAL == 0) {
                val timestamp = Instant.now()
                channelWriteManagerList.forEach { it.timestamp = timestamp }
            }
            numBatches++

            channelWriteManagerList.forEach { it.put(batch) }
            numProduced += batch.size
        }

        Log.debug("Source generated $numProduced records.", toString())
        Log.info("Closing buffers and quitting.", toString())
        channelWriteManagerList.forEach { it.close() }
    }

    override fun processBatch(batch: List<Unit>) = Unit
    override fun processRecord(record: Unit): OutputType {
        error("Unused function, should not have been called.")
    }
}
