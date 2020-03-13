package org.example.scalingstream.channels

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap

internal class ChannelBuilderTest {

    @Test
    fun localChannelBuilderTest() {
        val TEST_NAME = "${TEST_CLASS}->LocalBuilder"

        val channelArgs: ChannelArgs = HashMap<ChannelArg, Any>()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant? ,List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

        @SuppressWarnings("unused")
        val localChannelBuilder: ChannelBuilder = LocalChannelBuilder("$TEST_NAME:Builder", channelArgs)
        assertTrue(true)
    }

    @Test
    fun localChannelContextTest() {
        val TEST_NAME = "${TEST_CLASS}->LocalContext"

        val channelArgs: ChannelArgs = HashMap<ChannelArg, Any>()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant? ,List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

        val localChannelBuilder: ChannelBuilder = LocalChannelBuilder("$TEST_NAME", channelArgs)

        @SuppressWarnings("unused")
        val localContext: DataChannelContext = localChannelBuilder.buildChannelContext<Unit>("$TEST_NAME")
        assertTrue(true)
    }

    @Test
    fun localInputChannelTest() {
        val TEST_NAME = "${TEST_CLASS}->LocalInput"

        val channelArgs: ChannelArgs = HashMap<ChannelArg, Any>()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant? ,List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

        val localChannelBuilder: ChannelBuilder = LocalChannelBuilder("$TEST_NAME", channelArgs)

        localChannelBuilder.buildChannelContext<Unit>("$TEST_NAME")

        @SuppressWarnings("unused")
        val localInput = localChannelBuilder.buildInputChannel<Unit>("$TEST_NAME")
        assertTrue(true)
    }

    @Test
    fun localOutputChannelTest() {
        val TEST_NAME = "${Companion.TEST_CLASS}->LocalOutput"

        val channelArgs: ChannelArgs = HashMap<ChannelArg, Any>()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant? ,List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

        val localChannelBuilder: ChannelBuilder = LocalChannelBuilder("$TEST_NAME", channelArgs)

        localChannelBuilder.buildChannelContext<Unit>("$TEST_NAME")

        @SuppressWarnings("unused")
        val localOutput = localChannelBuilder.buildOutputChannel<Unit>("$TEST_NAME")
        assertTrue(true)
    }

    companion object {
        const val TEST_CLASS = "Test->ChannelBuilderTest"
    }
}
