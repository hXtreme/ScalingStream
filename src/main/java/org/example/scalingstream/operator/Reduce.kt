package org.example.scalingstream.operator

import org.example.scalingstream.control.channel.ChannelReaderManager
import org.example.scalingstream.control.channel.ChannelWriterManager
import java.util.*
import kotlin.collections.HashMap

class Reduce<KeyType, Type>(
    taskID: UUID,
    operatorID: String,
    channelReaderManagerList: List<ChannelReaderManager<Pair<KeyType, Type>>>,
    channelWriterManagerList: List<ChannelWriterManager<Type>>,
    operatorFn: (Pair<Type, Type>) -> Type
) : SingleInputTask<Pair<KeyType, Type>, Pair<Type, Type>, Type, Type>(
    taskID,
    operatorID,
    channelReaderManagerList,
    channelWriterManagerList,
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
