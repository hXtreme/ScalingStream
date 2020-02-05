package org.example.scalingstream.operator

import org.example.scalingstream.Record
import org.example.scalingstream.channels.*
import org.example.scalingstream.partitioner.RoundRobinPartitioner
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.util.*
import kotlin.collections.HashMap

internal class SourceTest {

    @Test
    fun source() {
        val channelArgs = HashMap<ChannelArgs, Any>()

        channelArgs[ChannelArgs.LOCAL_QUEUE_DICT] = HashMap<String, ArrayDeque<Record>>()
        channelArgs[ChannelArgs.MAX_QUEUE_LEN] = 2
        val s = Source(
            0,
            "0",
            listOf("1"),
            fun(x): Array<Any> { return arrayOf(0..x as Int) },
            RoundRobinPartitioner(2),
            1,
            1,
            ChannelBuilder(::LocalChannelContext, ::LocalInputChannel, ::LocalOutputChannel, channelArgs)
        )

        assert(true)
    }

    @Test
    fun run() {
        
    }
}