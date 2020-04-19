package page.parekh.scalingstream.examples.count

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import page.parekh.scalingstream.StreamContext
import page.parekh.scalingstream.channels.ChannelArg
import page.parekh.scalingstream.channels.ChannelArgs
import page.parekh.scalingstream.channels.ChannelBuilder
import page.parekh.scalingstream.channels.redis.RedisChannelBuilder
import page.parekh.scalingstream.executor.Executor
import page.parekh.scalingstream.executor.local.LocalExecutor
import page.parekh.scalingstream.executor.rpc.RPCExecutor

class CountMain() {
    companion object {
        fun runCountTest(
            executor: Executor,
            channelBuilder: (ChannelArgs) -> ChannelBuilder,
            channelArgs: ChannelArgs,
            batchSize: Int = 5,
            parallelism: Int = 3,
            countTo: Int = 100,
            printing: Boolean = false
        ) {
            val context = StreamContext(executor, channelBuilder(channelArgs), channelArgs, batchSize)
            val counter = Count(0, countTo)

            with(context.createStream("count", parallelism = parallelism) { counter.generator() }) {
                if (printing) {
                    this.print()
                } else {
                    this.drop()
                }
            }

            Log.info("Running Count")
            context.run()
            Log.info("Finished running Count.")
        }
    }
}

fun main() {
    Log.logLevel = LogLevel.ERROR
    val channelArgs: MutableMap<ChannelArg, Any> =
        mutableMapOf(Pair(ChannelArg.REDIS_HOST, "localhost"), Pair(ChannelArg.REDIS_PORT, 6379))

    CountMain.runCountTest(
        RPCExecutor(),
        ::RedisChannelBuilder,
        channelArgs,
        50,
        countTo = 1000000,
        printing = false
    )

}

