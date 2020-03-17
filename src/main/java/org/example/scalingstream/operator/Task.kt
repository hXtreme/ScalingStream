package org.example.scalingstream.operator

import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner


typealias TaskConstructor<InputType, FnInp, FnOut, OutputType> =
            (Int, String, List<String>, Int, ChannelBuilder, Int, Partitioner, (FnInp) -> FnOut)
        -> Task<InputType, FnInp, FnOut, OutputType>

typealias SimpleTask<InputType, OutputType> =
        Task<InputType, InputType, OutputType, OutputType>

typealias SimpleTaskConstructor<InputType, OutputType> =
        TaskConstructor<InputType, InputType, OutputType, OutputType>

abstract class Task<InputType, FnInp, FnOut, OutputType>(
    protected val taskID: Int,
    protected val operatorID: String,
    protected val outOperatorIDs: List<String>,
    protected val upstreamCount: Int,
    protected val channelBuilder: ChannelBuilder,
    protected val batchSize: Int,
    protected val partitioner: Partitioner,
    protected val operatorFn: (FnInp) -> FnOut
) {
    protected val numOut: Int = outOperatorIDs.size
    protected var numProcessed: Int = 0

    abstract fun run(): Any

    open fun processRecord(record: InputType): OutputType {
        error("Needs to be overridden")
    }

    protected open fun processRecordBatch(recordBatch: List<InputType>) {
        recordBatch.map { record -> processRecord(record) }
    }

    override fun toString(): String {
        return "${operatorID}${idx}_"
    }
}
