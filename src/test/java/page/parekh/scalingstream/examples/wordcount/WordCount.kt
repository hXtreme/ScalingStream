package page.parekh.scalingstream.examples.wordcount

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import page.parekh.scalingstream.StreamContext
import page.parekh.scalingstream.channels.ChannelBuilder
import page.parekh.scalingstream.channels.ChannelArg
import page.parekh.scalingstream.channels.local.LocalChannelBuilder
import page.parekh.scalingstream.executor.Executor
import page.parekh.scalingstream.executor.local.LocalExecutor
import page.parekh.scalingstream.partitioner.HashPartitioner
import page.parekh.scalingstream.stream.Stream
import java.io.File
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap

private const val NAME = "WordCount"

class WordCount(private val sentences: SentenceSource, batchSize: Int = 4, heavyHitterThreshold: Int = 1000) {
    private val words: Stream<Unit, String>
    private val context: StreamContext

    init {
        val executor: Executor = LocalExecutor()
        val id = Pair(UUID.randomUUID(), UUID.randomUUID())
        val channelArgs = HashMap<ChannelArg, Any>()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 10

        val channelBuilder: ChannelBuilder =
            LocalChannelBuilder(channelArgs)
        context = StreamContext(executor, channelBuilder, channelArgs, batchSize, ::HashPartitioner)

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

fun main(args: Array<String>) {
    val file = File(args.getOrElse(0) { "./README.md" })
    val numRecords = 10000
    val sentenceLength = 100
    val sentenceSource = SentenceSource(file, numRecords, sentenceLength)
    val threshold = (numRecords * sentenceLength) / 50
    val wordCount = WordCount(sentenceSource, 50, threshold)
    Log.logLevel = LogLevel.ERROR
    wordCount.run()
}
