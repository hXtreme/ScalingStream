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
import page.parekh.scalingstream.partitioner.HashPartitioner
import page.parekh.scalingstream.stream.Stream
import java.io.File
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap

private const val NAME = "WordCount"

class WordCount(
    private val sentences: SentenceSource,
    batchSize: Int = 4,
    heavyHitterThreshold: Int = 1000,
    channelArgs: ChannelArgs,
    channelBuilder: (ChannelArgs) -> ChannelBuilder = ::LocalChannelBuilder
) {
    private val words: Stream<Unit, String>
    private val context: StreamContext

    init {
        val executor: Executor = LocalExecutor()
        val id = Pair(UUID.randomUUID(), UUID.randomUUID())


        val cb = channelBuilder(channelArgs)
        context = StreamContext(executor, cb, channelArgs, batchSize, ::HashPartitioner)

        words = context.createStream(NAME) { sentences.generator() }
//        words.print()
        val count = words.flatMap {
            it.split(Regex("\\s"))
                .map { s -> Pair(s, 1) }
        }
            .keyBy { it.first }
            .reduce { (k, v1), (_, v2) -> Pair(k, v1 + v2) }

//        count.filter { (_, v) -> v > 50 }//.drop()
        count.filter { (k, v) -> v > heavyHitterThreshold && k !in setOf("ut", "#", "") }
            .map { (k, v) -> "Heavy Hitter($v): $k" }.print()
        count.filter { (_, v) -> v < heavyHitterThreshold }//.print()
            .drop()
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

    @Test
    fun wordCountTestWithRedisChannel() {
        val channelArgs = HashMap<ChannelArg, Any>()
        channelArgs[ChannelArg.REDIS_HOST] = "localhost"
        channelArgs[ChannelArg.REDIS_PORT] = 6379

        val sentenceSource = SentenceSource(file, numRecords, sentenceLength)
        val wordCount = WordCount(sentenceSource, 50, threshold, channelArgs, ::RedisChannelBuilder)
        wordCount.run()
    }

    @Test
    fun wordCountTestWithLocalChannel() {
        val channelArgs = HashMap<ChannelArg, Any>()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 10

        val sentenceSource = SentenceSource(file, numRecords, sentenceLength)
        val wordCount = WordCount(sentenceSource, 50, threshold, channelArgs, ::LocalChannelBuilder)
        wordCount.run()
    }
}
