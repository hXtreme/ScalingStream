package page.parekh.scalingstream.operator

import de.jupf.staticlog.Log
import page.parekh.scalingstream.control.channel.ChannelReaderManager
import page.parekh.scalingstream.control.channel.ChannelWriterManager
import page.parekh.scalingstream.extensions.*
import java.time.Duration
import java.time.Instant
import java.util.*

class Sink<InputType>(
    taskID: UUID,
    operatorID: String,
    channelReaderManagerList: List<ChannelReaderManager<InputType>>,
    channelWriterManagerList: List<ChannelWriterManager<Unit>>,
    operatorFn: (InputType) -> Unit
) : AbstractTask<InputType, InputType, Unit, Unit>(
    taskID,
    operatorID,
    channelReaderManagerList,
    emptyList(),
    operatorFn
) {

    override var isDone: Boolean = false
        private set

    override var isRunning: Boolean = false
        private set

    override fun run() {
        Log.info("Running sink task", toString())
        isRunning = true

        while (channelReaderManagerList.any { !it.isClosed }) {
            if (channelReaderManagerList.all { it.isEmpty }) Thread.sleep(100)
            while (channelReaderManagerList.any { !it.isEmpty }) {

                val selectedChannelReaderManager =
                    (inputChannelManagers.take(channelReaderManagerList.size).find { it.isNotEmpty })
                        ?: inputChannelManagers.first()
                val (timestamp, batch) = selectedChannelReaderManager.get()

                timestamp?.let { Log.debug("Latency: ${Duration.between(timestamp, Instant.now())}", name) }
                processBatch(batch)
                numConsumed += batch.size
            }
        }
        Log.debug("Processed $numConsumed records.", toString())
        Log.info("Closing buffers and quitting.", toString())
        channelReaderManagerList.forEach { it.close() }
        isDone = true
        isRunning = false
    }

    override fun processBatch(batch: List<InputType>) {
        batch.forEach { operatorFn(it) }
    }

    override fun processRecord(record: InputType) {
        error("Unused function, should not have been called.")
    }
}
