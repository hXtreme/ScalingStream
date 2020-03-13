package org.example.scalingstream.operator

import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner

class KeyBy<InputType, KeyType>(
    idx: Int,
    operatorID: String,
    outOperatorIDs: List<String>,
    upstreamCount: Int,
    channelBuilder: ChannelBuilder,
    batchSize: Int,
    partitioner: Partitioner,
    operatorFn: (InputType) -> KeyType
) : SingleInputOperator<InputType, InputType, KeyType, Pair<KeyType, InputType>>(
    idx,
    operatorID,
    outOperatorIDs,
    upstreamCount,
    channelBuilder,
    batchSize,
    partitioner,
    operatorFn
) {
    override fun processRecord(record: InputType): Pair<KeyType, InputType> {
        return Pair(operatorFn(record), record)
    }
}
