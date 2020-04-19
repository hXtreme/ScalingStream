package page.parekh.scalingstream.examples.count

import de.jupf.staticlog.Log
import de.jupf.staticlog.core.LogLevel
import org.junit.jupiter.api.Test
import page.parekh.scalingstream.channels.ChannelArg
import page.parekh.scalingstream.channels.Record
import page.parekh.scalingstream.channels.local.LocalChannelBuilder
import page.parekh.scalingstream.channels.redis.RedisChannelBuilder
import page.parekh.scalingstream.executor.local.LocalExecutor
import page.parekh.scalingstream.executor.rpc.RPCExecutor
import java.util.*
import kotlin.collections.HashMap

internal class CountTest() {

    init {
        Log.logLevel = LogLevel.ERROR
    }

    @Test
    fun localExecutionWithRedisChannel() {
        val channelArgs: MutableMap<ChannelArg, Any> =
            mutableMapOf(Pair(ChannelArg.REDIS_HOST, "localhost"), Pair(ChannelArg.REDIS_PORT, 6379))
        val count =
            Count(LocalExecutor(), ::RedisChannelBuilder, channelArgs, batchSize = 5, end = 100, printing = true)
        count.run()
    }

    @Test
    fun localExecutionWithLocalChannel() {
        val channelArgs: MutableMap<ChannelArg, Any> =
            mutableMapOf(Pair(ChannelArg.LOCAL_QUEUE_DICT, HashMap<String, Queue<Record<Any>>>()))
        val count =
            Count(LocalExecutor(), ::LocalChannelBuilder, channelArgs, batchSize = 5, end = 100, printing = false)
        count.run()
    }

    @Test
    fun rpcExecutionWithRedisChannel() {
        val channelArgs: MutableMap<ChannelArg, Any> =
            mutableMapOf(Pair(ChannelArg.REDIS_HOST, "localhost"), Pair(ChannelArg.REDIS_PORT, 6379))
        val count =
            Count(RPCExecutor(), ::RedisChannelBuilder, channelArgs, batchSize = 5, end = 100000, printing = true)
        count.run()
    }

    @Test
    fun rpcExecutionWithLocalChannel() {
        val channelArgs: MutableMap<ChannelArg, Any> =
            mutableMapOf(Pair(ChannelArg.LOCAL_QUEUE_DICT, HashMap<String, Queue<Record<Any>>>()))
        val count = Count(RPCExecutor(), ::LocalChannelBuilder, channelArgs, batchSize = 5, end = 100, printing = false)
        count.run()
    }
}
