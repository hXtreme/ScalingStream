package org.example.scalingstream.operator

import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner


abstract class Operator<InputType, OutputType>(
    protected val idx: Int,
    protected val operatorID: String,
    protected val outOperatorIDs: List<String>,
    protected val upstreamCount: Int,
    protected val channelBuilder: ChannelBuilder,
    protected val batchSize: Int,
    protected val partitioner: Partitioner,
    protected val operatorFn: (InputType) -> OutputType
) {
    protected val numOut: Int = outOperatorIDs.size
    protected var numProcessed: Int = 0

    abstract fun run(): Any

    protected open fun processRecord(record: InputType) {
        operatorFn(record)
    }

    protected open fun processRecordBatch(recordBatch: List<InputType>) {
        recordBatch.map { record -> processRecord(record) }
    }
}

