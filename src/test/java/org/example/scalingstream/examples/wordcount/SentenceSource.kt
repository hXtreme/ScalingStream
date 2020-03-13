package org.example.scalingstream.examples.wordcount

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import org.example.scalingstream.CONSTANTS
import org.example.scalingstream.StreamContext
import org.example.scalingstream.channels.ChannelArg
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.channels.LocalChannelBuilder
import org.example.scalingstream.executor.Executor
import org.example.scalingstream.executor.LocalExecutor
import java.io.File
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap


private const val NAME = "SentenceSource"

class SentenceSource(
    file: File,
    private val numRecords: Int,
    private val sentenceLength: Int,
    private val timestamp_interval: Duration? = null
) {
    private val words: List<String> = file.readText().split(Regex("\\s"))

    private var num: Int = 0

    fun generateSentence(): String {
        return (0 until sentenceLength).joinToString(separator = " ") { words.random() }
    }

    fun generator(): String? {
        return if (num++ < numRecords) generateSentence() else null
    }
}

fun main(args: Array<String>) {
    Log.logLevel = LogLevel.ERROR
    val file = File(args.getOrElse(0) { "./README.md" })
    val sentences = SentenceSource(file, 20, 2)

    val executor: Executor = LocalExecutor(NAME)
    val channelArgs = HashMap<ChannelArg, Any>()
    channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Pair<Instant?, List<Any>?>>>()
    channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

    val channelBuilder: ChannelBuilder = LocalChannelBuilder(NAME, channelArgs)
    val context = StreamContext(executor, channelBuilder, channelArgs)

    context.createStream(NAME) { sentences.generator() }.print()

    context.run()
}