package page.parekh.scalingstream.examples.wordcount

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import page.parekh.scalingstream.StreamContext
import page.parekh.scalingstream.channels.ChannelArg
import page.parekh.scalingstream.channels.ChannelBuilder
import page.parekh.scalingstream.channels.Record
import page.parekh.scalingstream.channels.local.LocalChannelBuilder
import page.parekh.scalingstream.executor.Executor
import page.parekh.scalingstream.executor.local.LocalExecutor
import java.io.File
import java.time.Duration
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

    @Synchronized
    fun generator(): String? {
        return if (num++ < numRecords) generateSentence() else null
    }
}

fun main(args: Array<String>) {
    Log.logLevel = LogLevel.ERROR
    val id = Pair(UUID.randomUUID(), UUID.randomUUID())
    val file = File(args.getOrElse(0) { "./README.md" })
    val sentences = SentenceSource(file, 20, 7)
    val executor: Executor = LocalExecutor()
    val channelArgs = HashMap<ChannelArg, Any>()
    channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Record<Any>>>()
    channelArgs[ChannelArg.MAX_QUEUE_LEN] = 2

    val channelBuilder: ChannelBuilder =
        LocalChannelBuilder(channelArgs)
    val context = StreamContext(executor, channelBuilder, channelArgs)

    context.createStream(NAME, parallelism = 5) { sentences.generator() }.print()

    context.run()
}
