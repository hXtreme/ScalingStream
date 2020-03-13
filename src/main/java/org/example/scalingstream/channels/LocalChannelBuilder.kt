package org.example.scalingstream.channels

import de.jupf.staticlog.Log
import java.time.Instant

import java.util.Queue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.collections.HashMap


object LocalChannelConstants {
    const val TYPE = "LOCAL"
}

open class LocalChannelBuilder(
    override val name: String,
    override val channelArgs: ChannelArgs
) : ChannelBuilder {

    override val type: String = LocalChannelConstants.TYPE

    override fun <Type> buildChannelContext(name: String): DataChannelContext {
        // For specialized typing inherit the class.
        Log.info("Building $type Channel Context: $name")
        return LocalChannelContext<Type>(name, channelArgs)
    }

    override fun <Type> buildInputChannel(name: String): InputChannel<Type> {
        Log.info("Building $type Input Channel: $name")
        return LocalInputChannel(name, channelArgs)
    }

    override fun <Type> buildOutputChannel(name: String): OutputChannel<Type> {
        Log.info("Building $type Output Channel: $name")
        return LocalOutputChannel(name, channelArgs)
    }
}

class LocalChannelContext<Type>(
    name: String,
    channelArgs: Map<ChannelArg, Any>
) : DataChannelContext(name, channelArgs) {
    private var queueDict: HashMap<String, Queue<Pair<Instant?, List<Type>?>>> =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as HashMap<String, Queue<Pair<Instant?, List<Type>?>>>
    private val maxQueueLength: Int = channelArgs.getOrDefault(ChannelArg.MAX_QUEUE_LEN, 0) as Int

    init {
        // TODO("Add size restriction")
        queueDict.putIfAbsent(name, LinkedBlockingQueue())
    }

    override fun destroy() {
        queueDict.remove(name)
    }
}

class LocalInputChannel<Type>(
    name: String,
    private val channelArgs: Map<ChannelArg, Any>
) : InputChannel<Type>(name) {
    private var queueDict: HashMap<String, BlockingQueue<Pair<Instant?, List<Type>?>>> =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as HashMap<String, BlockingQueue<Pair<Instant?, List<Type>?>>>
    private var q: BlockingQueue<Pair<Instant?, List<Type>?>>? = null

    override fun connect() {
        q = queueDict[name] ?: error("No queue named $name to connect to.")
    }

    override fun peek(): Pair<Instant?, List<Type>?>? {
        return q?.peek()
    }

    override fun get(): Pair<Instant?, List<Type>?> {
        var data = q!!.take()
        return data
    }
}

class LocalOutputChannel<Type>(
    name: String,
    private val channelArgs: Map<ChannelArg, Any>
) : OutputChannel<Type>(name) {
    private var queueDict: HashMap<String, BlockingQueue<Pair<Instant?, List<Type>?>>> =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as HashMap<String, BlockingQueue<Pair<Instant?, List<Type>?>>>
    private var q: BlockingQueue<Pair<Instant?, List<Type>?>>? = null

    override fun connect() {
        q = queueDict[name] ?: error("No queue named $name to connect to.")
    }

    override fun put(recordBatch: Pair<Instant?, List<Type>?>) {
        q!!.put(Pair(recordBatch.first, recordBatch.second?.toList()))
    }

    override fun flush() {
        // Do nothing
    }
}
