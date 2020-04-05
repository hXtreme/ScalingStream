package org.example.scalingstream.channels

import org.example.scalingstream.channels.local.LocalChannelBuilder
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap

internal class ChannelBuilderTest {

    @Test
    fun localChannelBuilderTest() {
        val channelArgs: ChannelArgs = HashMap()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

        @SuppressWarnings("unused")
        val localChannelBuilder: ChannelBuilder =
            LocalChannelBuilder(channelArgs)
        assertTrue(true)
    }

    @Test
    fun localChannelTest() {
        val channelID = ChannelID(UUID.randomUUID(), UUID.randomUUID())

        val channelArgs: ChannelArgs = HashMap()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

        val localChannelBuilder: ChannelBuilder =
            LocalChannelBuilder(channelArgs)

        @SuppressWarnings("unused")
        val localChannel: Channel<Unit> = localChannelBuilder.buildChannel(channelID)
        assertTrue(true)
    }

    @Test
    fun localInputChannelTest() {
        val channelID = ChannelID(UUID.randomUUID(), UUID.randomUUID())

        val channelArgs: ChannelArgs = HashMap()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

        val localChannelBuilder: ChannelBuilder =
            LocalChannelBuilder(channelArgs)

        val localChannel: Channel<Unit> = localChannelBuilder.buildChannel(channelID)

        @SuppressWarnings("unused")
        val localInput = localChannel.getChannelReader()
        assertTrue(true)
    }

    @Test
    fun localOutputChannelTest() {
        val channelID = ChannelID(UUID.randomUUID(), UUID.randomUUID())

        val channelArgs: ChannelArgs = HashMap()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

        val localChannelBuilder: ChannelBuilder =
            LocalChannelBuilder(channelArgs)

        val localChannel: Channel<Unit> = localChannelBuilder.buildChannel(channelID)

        @SuppressWarnings("unused")
        val localOutput = localChannel.getChannelWriter()
        assertTrue(true)
    }
}
