package org.example.scalingstream.operator

import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner

class FlatMap<InputType, OutputType>(
    idx: Int,
    operatorID: String,
    outOperatorIDs: List<String>,
    upstreamCount: Int,
    channelBuilder: ChannelBuilder,
    batchSize: Int,
    partitioner: Partitioner,
    operatorFn: (InputType) -> Iterable<OutputType>
) : SingleInputSimpleTransformationOperator<InputType,  Iterable<OutputType>, OutputType>(
    idx,
    operatorID,
    outOperatorIDs,
    upstreamCount,
    channelBuilder,
    batchSize,
    partitioner,
    operatorFn
) {
    override fun processRecordBatch(recordBatch: List<InputType>) {
        val flat = recordBatch.flatMap { it -> operatorFn(it) }
        partitioner.partitionBatch(outputBuffers, flat)
    }

    override fun processRecord(record: InputType): OutputType {
        error("Unused function: shouldn't have been called")
    }
}