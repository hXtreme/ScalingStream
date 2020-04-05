package page.parekh.scalingstream.operator

import de.jupf.staticlog.Log
import page.parekh.scalingstream.CONSTANTS
import page.parekh.scalingstream.control.channel.ChannelReaderManager
import page.parekh.scalingstream.control.channel.ChannelWriterManager
import java.time.Instant
import java.util.*


class Source<OutputType : Any>(
    taskID: UUID,
    operatorID: String,
    channelReaderManagerList: List<ChannelReaderManager<Unit>>,
    channelWriterManagerList: List<ChannelWriterManager<OutputType>>,
    operatorFn: (Unit) -> OutputType?
) : AbstractTask<Unit, Unit, OutputType?, OutputType>(
    taskID,
    operatorID,
    emptyList(),
    channelWriterManagerList,
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
                channelWriterManagerList.forEach { it.timestamp = timestamp }
            }
            numBatches++
            channelWriterManagerList.forEach { it.put(batch) }
            numProduced += batch.size
        }

        Log.debug("Source generated $numProduced records.", toString())
        Log.info("Closing buffers and quitting.", toString())
        channelWriterManagerList.forEach { it.close() }
    }

    override fun processBatch(batch: List<Unit>) = Unit
    override fun processRecord(record: Unit): OutputType {
        error("Unused function, should not have been called.")
    }
}
