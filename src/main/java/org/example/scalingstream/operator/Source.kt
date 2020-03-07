package org.example.scalingstream.operator

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
) : SimpleTransformationOperator<Unit, OutputType, OutputType>(
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
        output.forEach { it.connect() }

        var done = false

        while (!done) {
            val recordBatch: List<OutputType> = (0..batchSize).map { operatorFn(Unit) }
            outputBuffers.timestamp = if (numBatches % CONSTANTS.TIMESTAMP_INTERVAL == 0) Instant.now() else null
            if (recordBatch == CONSTANTS.DONE_MARKER) {
                done = true
            } else {
                partitioner.partitionBatch(outputBuffers, recordBatch)
                numProcessed += recordBatch.size
                numBatches++
            }
        }
        outputBuffers.close()
    }

    override fun processRecordBatch(recordBatch: List<Unit>) = Unit
}