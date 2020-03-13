package org.example.scalingstream.operator

import org.example.scalingstream.CONSTANTS
import org.example.scalingstream.channels.OutputChannel
import org.example.scalingstream.partitioner.Partitioner
import java.time.Instant

// TODO: Maybe move it to a different package
// TODO: Fix the type of output
class OutputBuffers<Type> internal constructor(
    private val batchSize: Int,
    private val output: List<OutputChannel<Type>>
) {

    private val numOut: Int = output.size
    private val outputBuffers: List<MutableList<Type>> = List(numOut) { mutableListOf<Type>() }

    var timestamp: Instant? = null

    fun append(idx: Int, record: Type) {
        val buffer = outputBuffers[idx]
        buffer.add(record)
        if (buffer.size == batchSize) {
            output[idx].put(Pair(timestamp, buffer)) // TODO: Fix the type here.
            buffer.removeAll { true }
            timestamp = null
        }
    }

    fun append(recordBatch: Pair<Instant?, List<Type>?>, partitioner: Partitioner) {
        val records: List<Type>? = recordBatch.second
        timestamp = recordBatch.first
        records!!.forEach { append(partitioner.assignPartition(it), it) }
    }


    fun close() {
        for (idx: Int in 0 until numOut) {
            if (outputBuffers[idx].isNotEmpty()) {
                output[idx].put(Pair(timestamp, outputBuffers[idx])) // TODO: Fix the type here.
            }
            output[idx].put(Pair(timestamp, CONSTANTS.DONE_MARKER)) // TODO: Fix the type here.
        }
    }
}
