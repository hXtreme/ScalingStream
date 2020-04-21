package page.parekh.scalingstream.examples.wordcount

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import org.junit.jupiter.api.Test
import page.parekh.scalingstream.channels.ChannelArg
import page.parekh.scalingstream.channels.local.LocalChannelBuilder
import page.parekh.scalingstream.channels.redis.RedisChannelBuilder
import page.parekh.scalingstream.executor.local.LocalExecutor
import page.parekh.scalingstream.executor.rpc.RPCExecutor
import java.io.File
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap

internal class WordCountTest() {
    val file = File("./README.md")
    private val numRecords = 10000
    private val sentenceLength = 100

    init {
        Log.logLevel = LogLevel.ERROR
    }

    @Test
    fun rpcExecutionWithRedisChannel() {
        val channelArgs = mutableMapOf<ChannelArg, Any>(
            Pair(ChannelArg.REDIS_HOST, "localhost"),
            Pair(ChannelArg.REDIS_PORT, 6379)
        )

        val wordCount = WordCount(
            RPCExecutor(),
            ::RedisChannelBuilder,
            channelArgs,
            file = file,
            numRecords = numRecords,
            sentenceLength = sentenceLength,
            batchSize = 50,
            parallelism = 4
        )
        wordCount.run()
    }

    @Test
    fun localExecutionWithRedisChannel() {
        val channelArgs = mutableMapOf<ChannelArg, Any>(
            Pair(ChannelArg.REDIS_HOST, "localhost"),
            Pair(ChannelArg.REDIS_PORT, 6379)
        )
        val wordCount = WordCount(
            LocalExecutor(),
            ::RedisChannelBuilder,
            channelArgs,
            file = file,
            numRecords = numRecords,
            sentenceLength = sentenceLength,
            batchSize = 50,
            parallelism = 4
        )
        wordCount.run()
    }

    @Test
    fun rpcExecutionWithLocalChannel() {
        val channelArgs = mutableMapOf<ChannelArg, Any>(
            Pair(
                ChannelArg.LOCAL_QUEUE_DICT,
                HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
            ),
            Pair(ChannelArg.MAX_QUEUE_LEN, 50)
        )
        val wordCount = WordCount(
            RPCExecutor(),
            ::LocalChannelBuilder,
            channelArgs,
            file = file,
            numRecords = numRecords,
            sentenceLength = sentenceLength,
            batchSize = 50,
            parallelism = 4
        )
        wordCount.run()
    }

    @Test
    fun localExecutionWithLocalChannel() {
        val channelArgs = mutableMapOf<ChannelArg, Any>(
            Pair(
                ChannelArg.LOCAL_QUEUE_DICT,
                HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
            ),
            Pair(ChannelArg.MAX_QUEUE_LEN, 50)
        )
        val wordCount = WordCount(
            LocalExecutor(),
            ::LocalChannelBuilder,
            channelArgs,
            file = file,
            numRecords = numRecords,
            sentenceLength = sentenceLength,
            batchSize = 50,
            parallelism = 4
        )
        wordCount.run()
    }
}