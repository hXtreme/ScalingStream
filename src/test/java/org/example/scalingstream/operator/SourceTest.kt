package org.example.scalingstream.operator

import org.example.scalingstream.channels.*
import org.example.scalingstream.partitioner.RoundRobinPartitioner
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap

internal class SourceTest {

    companion object {
        const val TEST_CLASS = "Test->SourceTest"
    }

    @Test
    fun buildSource() {
        val channelArgs = HashMap<ChannelArg, Any>()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

        val channelBuilder: ChannelBuilder = LocalChannelBuilder("${SinkTest.TEST_CLASS}->buildSource", channelArgs)
        @SuppressWarnings("unused")
        val s = Source(
            0,
            "0",
            listOf("1"),
            1,
            channelBuilder,
            1,
            RoundRobinPartitioner(2),
            (fun(_): Any { return 1 })
        )

        assertTrue(true)
    }

    @Test
    fun run() {

    }
}
