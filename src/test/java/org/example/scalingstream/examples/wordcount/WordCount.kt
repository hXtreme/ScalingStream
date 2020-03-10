package org.example.scalingstream.examples.wordcount

import org.example.scalingstream.StreamContext
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.channels.ChannelArg
import org.example.scalingstream.channels.LocalChannelBuilder
import org.example.scalingstream.executor.Executor
import org.example.scalingstream.executor.LocalExecutor
import org.example.scalingstream.stream.Stream
import java.io.File
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap

private const val NAME = "WordCount"

class WordCount(private val sentences: SentenceSource) {
    private val words: Stream<*, String?>
    private val context: StreamContext

    init {
        val executor: Executor = LocalExecutor(NAME)
        val channelArgs = HashMap<ChannelArg, Any>()
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
        channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

        val channelBuilder: ChannelBuilder = LocalChannelBuilder(NAME, channelArgs)
        context = StreamContext(executor, channelBuilder, channelArgs)

        words = context.createStream(NAME) { sentences.generator() }
        words.print()
        val count = words.flatMap {
                (it as String).split(Regex("\\s"))
                    .map { s -> Pair(s, 1) }
            }
            .keyBy { it.first }
            .reduce { (k, v1), (_, v2) -> Pair(k, v1 + v2) }

        count.filter { (_, v) -> v > 50 }
        count.print()
    }

    fun run() {
        context.run()
    }
}

fun main(args: Array<String>) {
    val file = File(args.getOrElse(0) { "./README.md" })
    val sentenceSource = SentenceSource(file, 100, 10)
    val wordCount = WordCount(sentenceSource)

    wordCount.run()
}
