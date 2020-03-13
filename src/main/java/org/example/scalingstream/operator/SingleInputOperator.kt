package org.example.scalingstream.operator

import de.jupf.staticlog.Log
import org.example.scalingstream.CONSTANTS
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner

typealias SingleInputSimpleOperator<InputType, OutputType> =
        SingleInputOperator<InputType, InputType, OutputType, OutputType>

abstract class SingleInputOperator<InputType, FnInp, FnOut, OutputType>(
    idx: Int,
    operatorID: String,
    outOperatorIDs: List<String>,
    upstreamCount: Int,
    channelBuilder: ChannelBuilder,
    batchSize: Int,
    partitioner: Partitioner,
    operatorFn: (FnInp) -> FnOut
) : Operator<InputType, FnInp, FnOut, OutputType>(
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
        Log.info("Running $idx instance of $operatorID")
        Log.info("$operatorID\t-->\t$operatorID\t-->\t${outOperatorIDs.joinToString(prefix="[", postfix="]" )}")
        inputChannel.connect()
        outputChannels.forEach { it.connect() }
        var done = false

        while (!done) {
            val (timestamp, recordBatch) = inputChannel.get()
            outputBuffers.timestamp = timestamp
            if (recordBatch == CONSTANTS.DONE_MARKER) {
                numDoneMarkers++
                done = numDoneMarkers == upstreamCount
            } else {
                processRecordBatch(recordBatch)
                numProcessed += recordBatch.size
            }
        }
        Log.info("Processed $numProcessed records. Closing buffers and quitting $operatorID${idx}_")
        outputBuffers.close()
    }

    override fun processRecordBatch(recordBatch: List<InputType>) {
            partitioner.partitionBatch(
                outputBuffers,
                recordBatch.map { record -> processRecord(record) }
            ) // TODO("Want to handle this via channel manager")
        /** TODO
         * Want the following interface:
         * channelManagers.forEach { it.put(recordBatch.map{record -> processRecord(record)}) }
         */
    }
}