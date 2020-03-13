package org.example.scalingstream.operator

import org.example.scalingstream.channels.*
import org.example.scalingstream.partitioner.RoundRobinPartitioner
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap

internal class SinkTest {

    companion object {
        const val TEST_CLASS = "Test->SinkTest"
    }

    @Test
    fun buildSink() {
        val channelArgs = HashMap<ChannelArg, Any>()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

        val channelBuilder: ChannelBuilder = LocalChannelBuilder("$TEST_CLASS->buildSink", channelArgs)

        @SuppressWarnings("unused")
        val sink = Sink(
            0,
            "0",
            listOf(),
            1,
            channelBuilder,
            1,
            RoundRobinPartitioner(1),
            fun(_: Unit) {}
        )

        assertTrue(true)
    }
}
