package org.example.scalingstream.operator

import org.example.scalingstream.CONSTANTS
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner


abstract class SingleInputOperator<InputType, OutputType>(
    idx: Int,
    operatorID: String,
    outOperatorIDs: List<String>,
    upstreamCount: Int,
    channelBuilder: ChannelBuilder,
    batchSize: Int,
    partitioner: Partitioner,
    operatorFn: (InputType) -> OutputType
) : Operator<InputType, OutputType>(
    idx,
    operatorID,
    outOperatorIDs,
    upstreamCount,
    channelBuilder,
    batchSize,
    partitioner,
    operatorFn
) {
    private val inputChannel = channelBuilder.buildInputChannel<InputType>(operatorID)
    private val outputChannels = outOperatorIDs
        .map { outOperatorID -> channelBuilder.buildOutputChannel<OutputType>(outOperatorID) }

    private val outputBuffers = OutputBuffers<OutputType>(batchSize, outputChannels)
    var numDoneMarkers = 0

    val state: HashMap<Any, Any> by lazy { HashMap<Any, Any>() }

    override fun run() {
        var done = false
        inputChannel.connect()
        outputChannels.forEach { it.connect() }

        while (!done) {
            val (timestamp, recordsBatch) = inputChannel.get()
            outputBuffers.timestamp = timestamp
            if (recordsBatch == CONSTANTS.DONE_MARKER) {
                numDoneMarkers++
                done = numDoneMarkers == upstreamCount
            } else {
                processRecordBatch(recordsBatch)
                numProcessed += recordsBatch.size
            }
        }
    }
}