package org.example.scalingstream.operator

import org.example.scalingstream.CONSTANTS
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner

typealias SingleInputSimpleTransformationOperator<InputType, FnOut, OutputType> =
        SingleInputOperator<InputType, InputType, FnOut, OutputType>

abstract class SingleInputOperator<InputType, FnInp, FnOut, OutputType>(
    idx: Int,
    operatorID: String,
    outOperatorIDs: List<String>,
    upstreamCount: Int,
    channelBuilder: ChannelBuilder,
    batchSize: Int,
    partitioner: Partitioner,
    operatorFn: (FnInp) -> FnOut
) : Operator<InputType, FnInp, FnOut ,OutputType>(
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

    protected val outputBuffers = OutputBuffers<OutputType>(batchSize, outputChannels)
    private var numDoneMarkers = 0

    // protected val state: HashMap<Any, Any> by lazy { HashMap<Any, Any>() }

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

    override fun processRecordBatch(recordBatch: List<InputType>) {
        partitioner.partitionBatch(
            outputBuffers,
            recordBatch.map { record -> processRecord(record) }
        )
    }
}