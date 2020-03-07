package org.example.scalingstream.operator

import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner

class Map<InputType, OutputType>(
    idx: Int,
    operatorID: String,
    outOperatorIDs: List<String>,
    upstreamCount: Int,
    channelBuilder: ChannelBuilder,
    batchSize: Int,
    partitioner: Partitioner,
    operatorFn: (InputType) -> OutputType
) : SingleInputSimpleOperator<InputType, OutputType>(
    idx,
    operatorID,
    outOperatorIDs,
    upstreamCount,
    channelBuilder,
    batchSize,
    partitioner,
    operatorFn
) {
    override fun processRecord(record: InputType): OutputType {
        return operatorFn(record)
    }
}