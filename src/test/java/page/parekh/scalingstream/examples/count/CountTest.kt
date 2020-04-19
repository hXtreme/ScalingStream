package page.parekh.scalingstream.examples.count

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import org.junit.jupiter.api.Test
import page.parekh.scalingstream.StreamContext
import page.parekh.scalingstream.channels.ChannelArg
import page.parekh.scalingstream.channels.ChannelArgs
import page.parekh.scalingstream.channels.ChannelBuilder
import page.parekh.scalingstream.channels.Record
import page.parekh.scalingstream.channels.local.LocalChannelBuilder
import page.parekh.scalingstream.channels.redis.RedisChannelBuilder
import page.parekh.scalingstream.executor.Executor
import page.parekh.scalingstream.executor.local.LocalExecutor
import page.parekh.scalingstream.executor.rpc.RPCExecutor
import java.util.*
import kotlin.collections.HashMap

internal class Count(private val start: Int = 0, private val end: Int = Int.MAX_VALUE) {
    private var n = start

    @Synchronized
    fun generator(): Int? {
        return if (n <= end) n++ else null
    }
}

internal class CountTest() {

    init {
        Log.logLevel = LogLevel.ERROR
    }

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

    @Test
    fun localExecutionWithRedisChannel() {
        val channelArgs: MutableMap<ChannelArg, Any> =
            mutableMapOf(Pair(ChannelArg.REDIS_HOST, "localhost"), Pair(ChannelArg.REDIS_PORT, 6379))

        runCountTest(LocalExecutor(), ::RedisChannelBuilder, channelArgs, 5, countTo = 100, printing = true)
    }

    @Test
    fun localExecutionWithLocalChannel() {
        val channelArgs: MutableMap<ChannelArg, Any> =
            mutableMapOf(Pair(ChannelArg.LOCAL_QUEUE_DICT, HashMap<String, Queue<Record<Any>>>()))

        runCountTest(LocalExecutor(), ::LocalChannelBuilder, channelArgs, 5, countTo = 100, printing = false)
    }

    @Test
    fun rpcExecutionWithRedisChannel() {
        val channelArgs: MutableMap<ChannelArg, Any> =
            mutableMapOf(Pair(ChannelArg.REDIS_HOST, "localhost"), Pair(ChannelArg.REDIS_PORT, 6379))

        runCountTest(RPCExecutor(), ::RedisChannelBuilder, channelArgs, 5, countTo = 100, printing = true)
    }

    @Test
    fun rpcExecutionWithLocalChannel() {
        val channelArgs: MutableMap<ChannelArg, Any> =
            mutableMapOf(Pair(ChannelArg.LOCAL_QUEUE_DICT, HashMap<String, Queue<Record<Any>>>()))

        runCountTest(RPCExecutor(), ::LocalChannelBuilder, channelArgs, 5, countTo = 100, printing = false)
    }
}
