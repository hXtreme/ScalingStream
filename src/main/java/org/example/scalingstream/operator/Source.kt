package org.example.scalingstream.operator

import org.example.scalingstream.CONSTANTS
import org.example.scalingstream.Record
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner
import java.time.Instant


class Source(
    idx: Int,
    operatorID: String,
    outOperatorIDs: List<String>,
    _operatorFn: (Any) -> Array<Any>,
    partitioner: Partitioner,
    upstreamCount: Int,
    batchSize: Int,
    channelBuilder: ChannelBuilder
) : Operator(idx, operatorID, outOperatorIDs, _operatorFn, partitioner, upstreamCount, batchSize, channelBuilder) {

    private var numBatches = 0
    private val output = outOperatorIDs.map { channelBuilder.buildOutputChannel(it) }
    private val outputBuffers = OutputBuffers(batchSize, output)

    override fun run() {
        output.forEach { it.connect() }

        for (recordBatch: Array<Record> in operatorFn) {
            outputBuffers.timestamp =
                if (numBatches % CONSTANTS.TIMESTAMP_INTERVAL == 0) Instant.now().epochSecond.toInt() else 0
            partitioner.assignPartition(outputBuffers, recordBatch)
            numProcessed += recordBatch.size
            numBatches++
        }
        outputBuffers.close()
    }
}