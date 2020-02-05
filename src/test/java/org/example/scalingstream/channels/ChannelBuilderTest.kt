package org.example.scalingstream.channels

import org.example.scalingstream.Record
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.collections.HashMap

internal class ChannelBuilderTest {

    @Test
    fun buildChannelContext() {
        val channelArgs = HashMap<ChannelArgs, Any>()

        channelArgs[ChannelArgs.LOCAL_QUEUE_DICT] = HashMap<String, ArrayDeque<Record>>()
        channelArgs[ChannelArgs.MAX_QUEUE_LEN] = 2

        val cb = ChannelBuilder(
            ::LocalChannelContext,
            ::LocalInputChannel,
            ::LocalOutputChannel,
            channelArgs
        )

        val ctx = cb.buildChannelContext("Test->buildChannelContext")
        assert(true)
    }

    @Test
    fun buildInputChannel() {
        val channelArgs = HashMap<ChannelArgs, Any>()

        channelArgs[ChannelArgs.LOCAL_QUEUE_DICT] = HashMap<String, ArrayDeque<Record>>()
        channelArgs[ChannelArgs.MAX_QUEUE_LEN] = 2

        val cb = ChannelBuilder(
            ::LocalChannelContext,
            ::LocalInputChannel,
            ::LocalOutputChannel,
            channelArgs
        )

        val inpChannel = cb.buildInputChannel("Test->buildInputChannel")
        assert(true)
    }

    @Test
    fun buildOutputChannel() {
        val channelArgs = HashMap<ChannelArgs, Any>()

        channelArgs[ChannelArgs.LOCAL_QUEUE_DICT] = HashMap<String, ArrayDeque<Record>>()
        channelArgs[ChannelArgs.MAX_QUEUE_LEN] = 2

        val cb = ChannelBuilder(
            ::LocalChannelContext,
            ::LocalInputChannel,
            ::LocalOutputChannel,
            channelArgs
        )

        val outChannel = cb.buildOutputChannel("Test->buildOutputChannel")
        assert(true)
    }
}