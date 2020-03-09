package org.example.scalingstream.channels

import java.time.Instant

import java.util.Queue
import java.util.ArrayDeque
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
        return LocalChannelContext<Any>(name, channelArgs)
    }

    override fun <Type> buildInputChannel(name: String): InputChannel<Type> {
        return LocalInputChannel(name, channelArgs)
    }

    override fun <Type> buildOutputChannel(name: String): OutputChannel<Type> {
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
        queueDict[name] = ArrayDeque<Pair<Instant?, List<Type>?>>()
    }

    override fun destroy() {
        queueDict.remove(name)
    }
}

class LocalInputChannel<Type>(
    name: String,
    private val channelArgs: Map<ChannelArg, Any>
) : InputChannel<Type>(name) {
    private var queueDict: HashMap<String, Queue<Pair<Instant?, List<Type>?>>> =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as HashMap<String, Queue<Pair<Instant?, List<Type>?>>>
    private var q: Queue<Pair<Instant?, List<Type>?>>? = null

    override fun connect() {
        q = queueDict[name] ?: error("No queue named $name to connect to.")
    }

    override fun get(): Pair<Instant?, List<Type>?> {
        return q!!.first()
    }
}

class LocalOutputChannel<Type>(
    name: String,
    private val channelArgs: Map<ChannelArg, Any>
) : OutputChannel<Type>(name) {
    private var queueDict: HashMap<String, Queue<Pair<Instant?, List<Type>?>>> =
        channelArgs[ChannelArg.LOCAL_QUEUE_DICT] as HashMap<String, Queue<Pair<Instant?, List<Type>?>>>
    private var q: Queue<Pair<Instant?, List<Type>?>>? = null

    override fun connect() {
        q = queueDict[name] ?: error("No queue named $name to connect to.")
    }

    override fun put(record: Pair<Instant?, List<Type>?>) {
        q!!.add(record)
    }

    override fun flush() {
        // Do nothing
    }
}

