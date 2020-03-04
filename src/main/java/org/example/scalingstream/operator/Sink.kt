package org.example.scalingstream.operator

import org.example.scalingstream.CONSTANTS
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.channels.InputChannel
import org.example.scalingstream.partitioner.Partitioner

import de.jupf.staticlog.Log
import java.time.Duration
import java.time.Instant

class Sink<InputType>(
    idx: Int,
    operatorID: String,
    outOperatorIDs: List<String>,
    upstreamCount: Int,
    channelBuilder: ChannelBuilder,
    partitioner: Partitioner,
    batchSize: Int,
    operatorFn: (Any?) -> Unit
) : Operator<InputType, Unit>(
    idx,
    operatorID,
    outOperatorIDs,
    upstreamCount,
    channelBuilder,
    batchSize,
    partitioner,
    operatorFn
) {

    private val input: InputChannel<InputType> = channelBuilder.buildInputChannel(operatorID)
    private var numDoneMarkers: Int = 0

    override fun run() {
        input.connect()
        var done = false

        while (!done) {
            val (timestamp, recordBatch) = input.get()
            timestamp?.let { Log.info("Latency: ${Duration.between(timestamp, Instant.now())}") }
            if (recordBatch == CONSTANTS.DONE_MARKER) {
                numDoneMarkers++
                done = numDoneMarkers == upstreamCount
            } else {
                processRecordBatch(recordBatch)
                numProcessed++
            }
        }
    }
}