package org.example.scalingstream.operator

import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner

class Reduce<KeyType, Type>(
    idx: Int,
    operatorID: String,
    outOperatorIDs: List<String>,
    upstreamCount: Int,
    channelBuilder: ChannelBuilder,
    batchSize: Int,
    partitioner: Partitioner,
    operatorFn: (Pair<Type, Type>) -> Type
) : SingleInputOperator<Pair<KeyType, Type>, Pair<Type, Type>, Type, Type>(
    idx,
    operatorID,
    outOperatorIDs,
    upstreamCount,
    channelBuilder,
    batchSize,
    partitioner,
    operatorFn
) {
    private val state: HashMap<KeyType, Type> = HashMap()

    override fun processRecord(record: Pair<KeyType, Type>): Type {
        val (k, v) = record
        state.computeIfPresent(k, { _, v1 -> operatorFn(Pair(v1, v)) })
        state.computeIfAbsent(k, { v })
        return state[k]!!
    }
}
