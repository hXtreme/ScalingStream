package page.parekh.scalingstream.examples.wordcount

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import page.parekh.scalingstream.StreamContext
import page.parekh.scalingstream.channels.*
import page.parekh.scalingstream.channels.redis.RedisChannelBuilder
import page.parekh.scalingstream.executor.Executor
import page.parekh.scalingstream.executor.local.LocalExecutor
import page.parekh.scalingstream.executor.rpc.RPCExecutor
import page.parekh.scalingstream.partitioner.HashPartitioner
import java.io.File

private const val NAME = "WordCount"

class WordCount(
    executor: Executor,
    channelBuilder: (ChannelArgs) -> ChannelBuilder,
    channelArgs: ChannelArgs,
    file: File = File("./README.md"),
    numRecords: Int = 100000,
    sentenceLength: Int = 100,
    batchSize: Int = 4,
    parallelism: Int = 5,
    printing: Boolean = false
) {
    private val sentenceSource = SentenceSource(file, numRecords, sentenceLength)
    private val context: StreamContext =
        StreamContext(executor, channelBuilder(channelArgs), channelArgs, batchSize, ::HashPartitioner)


    init {
        val sentences = context.createStream(NAME, parallelism = parallelism) { sentenceSource.generator() }
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

fun main() {
    Log.logLevel = LogLevel.ERROR
    val channelArgs = mutableMapOf<ChannelArg, Any>(
        Pair(ChannelArg.REDIS_HOST, "localhost"),
        Pair(ChannelArg.REDIS_PORT, 6379)
    )

    val wordCount = WordCount(LocalExecutor(), ::RedisChannelBuilder, channelArgs, batchSize = 50, parallelism = 4)
    wordCount.run()
}
