package page.parekh.scalingstream.examples.count

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import page.parekh.scalingstream.StreamContext
import page.parekh.scalingstream.channels.ChannelArg
import page.parekh.scalingstream.channels.ChannelBuilder
import page.parekh.scalingstream.channels.Record
import page.parekh.scalingstream.channels.local.LocalChannelBuilder
import page.parekh.scalingstream.executor.Executor
import page.parekh.scalingstream.executor.local.LocalExecutor
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
