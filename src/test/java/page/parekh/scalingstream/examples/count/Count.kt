package page.parekh.scalingstream.examples.count

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import page.parekh.scalingstream.StreamContext
import page.parekh.scalingstream.channels.ChannelArg
import page.parekh.scalingstream.channels.ChannelArgs
import page.parekh.scalingstream.channels.ChannelBuilder
import page.parekh.scalingstream.channels.redis.RedisChannelBuilder
import page.parekh.scalingstream.executor.Executor
import page.parekh.scalingstream.executor.rpc.RPCExecutor

internal class Count(
    executor: Executor,
    channelBuilder: (ChannelArgs) -> ChannelBuilder,
    channelArgs: ChannelArgs,
    batchSize: Int = 5,
    parallelism: Int = 3,
    private val start: Int = 0,
    private val end: Int = Int.MAX_VALUE,
    printing: Boolean = false
) {
    private var n = start

    @Synchronized
    fun generator(): Int? {
        return if (n <= end) n++ else null
    }

    private val context: StreamContext = StreamContext(executor, channelBuilder(channelArgs), channelArgs, batchSize)

    init {
        with(context.createStream("count", parallelism = parallelism) { generator() }) {
            if (printing) {
                this.print()
            } else {
                this.drop()
            }
        }
    }

    fun run() {
        Log.info("Running Count")
        context.run()
        Log.info("Finished running Count.")
    }
}

fun main() {
    Log.logLevel = LogLevel.WARN
    val channelArgs: MutableMap<ChannelArg, Any> =
        mutableMapOf(Pair(ChannelArg.REDIS_HOST, "localhost"), Pair(ChannelArg.REDIS_PORT, 6379))
    val count = Count(RPCExecutor(), ::RedisChannelBuilder, channelArgs, 50, end = 100, printing = false)
    count.run()
}
