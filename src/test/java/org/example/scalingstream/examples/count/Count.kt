package org.example.scalingstream.examples.count

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import org.example.scalingstream.StreamContext
import org.example.scalingstream.channels.ChannelArg
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.channels.Record
import org.example.scalingstream.channels.local.LocalChannelBuilder
import org.example.scalingstream.executor.Executor
import org.example.scalingstream.executor.local.LocalExecutor
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.collections.HashMap

internal class Count(private val start: Int = 0, private val end: Int = Int.MAX_VALUE) {
    private var n = start

    @Synchronized
    fun generator(): Int? {
        return if (n <= end) n++ else null
    }

}

fun main() {

    Log.logLevel = LogLevel.INFO
    val channelArgs: MutableMap<ChannelArg, Any> = HashMap()
    channelArgs[ChannelArg.LOCAL_QUEUE_DICT] = HashMap<String, Queue<Record<Any>>>()
    val channelBuilder: ChannelBuilder = LocalChannelBuilder(channelArgs)

    val executor: Executor = LocalExecutor()
    val context = StreamContext(executor, channelBuilder, channelArgs, 5)

    val counter = Count(0, 100)
    context.createStream("count", parallelism = 5) { counter.generator() }.print()

    context.run()
}
