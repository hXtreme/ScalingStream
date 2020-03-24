package org.example.scalingstream.operator

import org.example.scalingstream.control.channel.ChannelReadManager
import org.example.scalingstream.control.channel.ChannelWriteManager
import java.util.*
import kotlin.collections.HashMap

class Reduce<KeyType, Type>(
    taskID: UUID,
    operatorID: String,
    channelReadManagerList: List<ChannelReadManager<Pair<KeyType, Type>>>,
    channelWriteManagerList: List<ChannelWriteManager<Type>>,
    operatorFn: (Pair<Type, Type>) -> Type
) : SingleInputTask<Pair<KeyType, Type>, Pair<Type, Type>, Type, Type>(
    taskID,
    operatorID,
    channelReadManagerList,
    channelWriteManagerList,
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
