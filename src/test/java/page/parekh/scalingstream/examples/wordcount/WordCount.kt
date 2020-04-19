package page.parekh.scalingstream.examples.wordcount

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import org.junit.jupiter.api.Test
import page.parekh.scalingstream.StreamContext
import page.parekh.scalingstream.channels.*
import page.parekh.scalingstream.channels.local.LocalChannelBuilder
import page.parekh.scalingstream.channels.redis.RedisChannelBuilder
import page.parekh.scalingstream.executor.Executor
import page.parekh.scalingstream.executor.local.LocalExecutor
import page.parekh.scalingstream.executor.rpc.RPCExecutor
import page.parekh.scalingstream.partitioner.HashPartitioner
import page.parekh.scalingstream.stream.Stream
import java.io.File
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap

private const val NAME = "WordCount"

class WordCount(
    private val sentenceSource: SentenceSource,
    executor: Executor,
    channelBuilder: (ChannelArgs) -> ChannelBuilder,
    channelArgs: ChannelArgs,
    batchSize: Int = 4,
    parallelism: Int = 5,
    printing: Boolean = false
) {
    private val sentences: Stream<Unit, String>
    private val context: StreamContext

    init {
        val cb = channelBuilder(channelArgs)
        context = StreamContext(executor, cb, channelArgs, batchSize, ::HashPartitioner)

        sentences = context.createStream(NAME, parallelism = parallelism) { sentenceSource.generator() }
        val wordCount = sentences
            .flatMap {
                it.split(Regex("\\s"))
                    .map { s -> Pair(s, 1) }
            }
            .keyBy { it.first }
            .reduce { (k, v1), (_, v2) -> Pair(k, v1 + v2) }

        with(wordCount) {
            if (printing)
                this.print()
            else
                this.drop()
        }
    }

    fun run() {
        context.run()
    }
}


internal class WordCountTest() {
    val file = File("./README.md")
    private val numRecords = 10000
    private val sentenceLength = 100
    private val threshold = (numRecords * sentenceLength) / 50

    init {
        Log.logLevel = LogLevel.ERROR
    }

    fun runWordCountTest(
        executor: Executor,
        channelBuilder: (ChannelArgs) -> ChannelBuilder,
        channelArgs: ChannelArgs,
        batchSize: Int = 5,
        parallelism: Int = 3,
        printing: Boolean = false,
        numRecords: Int = this.numRecords,
        sentenceLength: Int = this.sentenceLength
    ) {
        val sentenceSource = SentenceSource(file, numRecords, sentenceLength)
        val wordCount = WordCount(sentenceSource, executor, channelBuilder, channelArgs, batchSize, parallelism, printing)

        wordCount.run()
    }

    @Test
    fun rpcExecutionWithRedisChannel() {
        val channelArgs = mutableMapOf<ChannelArg, Any>(
            Pair(ChannelArg.REDIS_HOST, "localhost"),
            Pair(ChannelArg.REDIS_PORT, 6379)
        )

        runWordCountTest(RPCExecutor(), ::RedisChannelBuilder, channelArgs, 50, 4)
    }

    @Test
    fun localExecutionWithRedisChannel() {
        val channelArgs = mutableMapOf<ChannelArg, Any>(
            Pair(ChannelArg.REDIS_HOST, "localhost"),
            Pair(ChannelArg.REDIS_PORT, 6379)
        )

        runWordCountTest(LocalExecutor(), ::RedisChannelBuilder, channelArgs, 50, 4)
    }

    @Test
    fun rpcExecutionWithLocalChannel() {
        val channelArgs = mutableMapOf<ChannelArg, Any>(
            Pair(ChannelArg.LOCAL_QUEUE_DICT, HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()),
            Pair(ChannelArg.MAX_QUEUE_LEN, 50)
        )

        runWordCountTest(RPCExecutor(), ::LocalChannelBuilder, channelArgs, 50, 4)
    }

    @Test
    fun localExecutionWithLocalChannel() {
        val channelArgs = mutableMapOf<ChannelArg, Any>(
            Pair(ChannelArg.LOCAL_QUEUE_DICT, HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()),
            Pair(ChannelArg.MAX_QUEUE_LEN, 50)
        )

        runWordCountTest(LocalExecutor(), ::LocalChannelBuilder, channelArgs, 50, 4)
    }
}
