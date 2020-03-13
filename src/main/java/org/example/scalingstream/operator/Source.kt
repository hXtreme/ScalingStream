package org.example.scalingstream.operator

import de.jupf.staticlog.Log
import org.example.scalingstream.CONSTANTS
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner
import java.time.Instant


class Source<OutputType>(
    idx: Int,
    operatorID: String,
    outOperatorIDs: List<String>,
    upstreamCount: Int,
    channelBuilder: ChannelBuilder,
    batchSize: Int,
    partitioner: Partitioner,
    operatorFn: (Unit) -> OutputType
) : SimpleOperator<Unit, OutputType>(
    idx,
    operatorID,
    outOperatorIDs,
    upstreamCount,
    channelBuilder,
    batchSize,
    partitioner,
    operatorFn
) {

    private var numBatches = 0
    private val output = outOperatorIDs.map { channelBuilder.buildOutputChannel<OutputType>(it) }
    private val outputBuffers = OutputBuffers<OutputType>(batchSize, output)

    override fun run() {
        Log.info("Running $operatorID$idx")
        Log.info("$operatorID --> $operatorID --> ${outOperatorIDs.joinToString()}")
        output.forEach { it.connect() }

        var done = false

        while (!done) {
            val inp = (0 until batchSize).map { operatorFn(Unit) }.filter{it != null}
            val recordBatch: List<OutputType>? = if (inp.isNotEmpty()) inp else null

            outputBuffers.timestamp =
                if (numBatches % CONSTANTS.TIMESTAMP_INTERVAL == 0) Instant.now()
                else outputBuffers.timestamp
            if (recordBatch == CONSTANTS.DONE_MARKER) {
                done = true
            } else {
                partitioner.partitionBatch(outputBuffers, recordBatch) // TODO("Want to handle this via channel manager")
                numProcessed += recordBatch.size
                numBatches++
            }
        }
        Log.info("Processed $numProcessed records. Closing buffers and quitting $operatorID${idx}_")
        outputBuffers.close()
    }

    override fun processRecordBatch(recordBatch: List<Unit>) = Unit
}
