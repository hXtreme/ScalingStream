package page.parekh.scalingstream.examples.count

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import org.junit.jupiter.api.Test
import page.parekh.scalingstream.StreamContext
import page.parekh.scalingstream.channels.ChannelArg
import page.parekh.scalingstream.channels.ChannelBuilder
import page.parekh.scalingstream.channels.Record
import page.parekh.scalingstream.channels.local.LocalChannelBuilder
import page.parekh.scalingstream.channels.redis.RedisChannelBuilder
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

internal class CountTest() {

    init {
        Log.logLevel = LogLevel.ERROR
    }

    @Test
    fun countWithRedisChannel() {
        val channelArgs: MutableMap<ChannelArg, Any> =
            mutableMapOf(Pair(ChannelArg.REDIS_HOST, "localhost"), Pair(ChannelArg.REDIS_PORT, 6379))

        val executor: Executor = LocalExecutor()
        val channelBuilder: ChannelBuilder = RedisChannelBuilder(channelArgs)
        val context: StreamContext = StreamContext(executor, channelBuilder, channelArgs, 5)
        val counter: Count = Count(0, 100)

        context.createStream("count", parallelism = 5) { counter.generator() }.print()
        Log.info("Running Count with Redis Channels.")
        context.run()
        Log.info("Finished running Count with redis Channels.")
    }

    @Test
    fun countWithLocalChannel() {
        val channelArgs: MutableMap<ChannelArg, Any> =
            mutableMapOf(Pair(ChannelArg.LOCAL_QUEUE_DICT, HashMap<String, Queue<Record<Any>>>()))

        val executor: Executor = LocalExecutor()
        val channelBuilder: ChannelBuilder = LocalChannelBuilder(channelArgs)
        val context: StreamContext = StreamContext(executor, channelBuilder, channelArgs, 5)
        val counter: Count = Count(0, 100)

        context.createStream("count", parallelism = 5) { counter.generator() }.print()
        Log.info("Running Count with Local Channels.")
        context.run()
        Log.info("Finished running Count with Local Channels.")
    }
}
