package org.example.scalingstream.operator

import org.example.scalingstream.Record
import org.example.scalingstream.channels.OutputChannel

// TODO: Maybe move it to a different package
// TODO: Fix the type of output
class OutputBuffers internal constructor(
    private val batchSize: Int,
    private val output: List<OutputChannel>
) {

    private val numOut: Int = output.size
    private val outputBuffers: List<MutableList<Record>> = (0..numOut).map { mutableListOf<Record>() }

    var timestamp: Int = 0

    fun append(idx: Int, record: Record) {
        val buffer = outputBuffers[idx]
        buffer.add(record)
        if (buffer.size == batchSize) {
            output[idx].put(Pair(timestamp, buffer) as Record) // TODO: Fix the type here.
            buffer.removeAll { true }
            timestamp = 0
        }
    }


    fun close() {
        
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}