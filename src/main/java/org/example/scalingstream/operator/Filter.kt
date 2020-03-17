package org.example.scalingstream.operator

import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner

class Filter<InputType>(
    taskID: Int,
    operatorID: String,
    outOperatorIDs: List<String>,
    upstreamCount: Int,
    channelBuilder: ChannelBuilder,
    batchSize: Int,
    partitioner: Partitioner,
    operatorFn: (InputType) -> Boolean
) : SingleInputTask<InputType, InputType, Boolean, InputType>(
    taskID,
    operatorID,
    outOperatorIDs,
    upstreamCount,
    channelBuilder,
    batchSize,
    partitioner,
    operatorFn
) {
    override fun processRecordBatch(recordBatch: List<InputType>) {
        partitioner.partitionBatch(
            outputBuffers,
            recordBatch.filter { record -> operatorFn(record) }
        )
    }
}
