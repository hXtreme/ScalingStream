package org.example.scalingstream.operator

import org.example.scalingstream.control.channel.InputChannelManager
import org.example.scalingstream.control.channel.OutputChannelManager

class Reduce<KeyType, Type>(
    taskID: Int,
    operatorID: String,
    inputChannelManagers: List<InputChannelManager<Pair<KeyType, Type>>>,
    outputChannelManagers: List<OutputChannelManager<Type>>,
    operatorFn: (Pair<Type, Type>) -> Type
) : SingleInputTask<Pair<KeyType, Type>, Pair<Type, Type>, Type, Type>(
    taskID,
    operatorID,
    inputChannelManagers,
    outputChannelManagers,
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
