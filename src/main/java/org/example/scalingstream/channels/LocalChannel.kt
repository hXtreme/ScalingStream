package org.example.scalingstream.channels

import org.example.scalingstream.Record

import java.util.Queue
import java.util.ArrayDeque
import kotlin.collections.HashMap

class LocalChannelContext(
    name: String,
    private val channelArgs: Map<ChannelArgs, Any>
) : DataChannelContext(name) {
    private var queueDict: HashMap<String, Queue<Record>> =
        channelArgs[ChannelArgs.LOCAL_QUEUE_DICT] as HashMap<String, Queue<Record>>
    private val maxQueueLength: Int = channelArgs.getOrDefault(ChannelArgs.MAX_QUEUE_LEN, 0) as Int

    override fun init() {
        // TODO("Add size restriction")
        queueDict[name] = ArrayDeque<Record>()
    }

    override fun destroy() {
        queueDict.remove(name)
    }
}

class LocalInputChannel(
    name: String,
    private val channelArgs: Map<ChannelArgs, Any>
) : InputChannel(name) {
    private var queueDict: HashMap<String, Queue<Record>> =
        channelArgs[ChannelArgs.LOCAL_QUEUE_DICT] as HashMap<String, Queue<Record>>
    private var q: Queue<Record>? = null

    override fun connect() {
        q = queueDict[name] ?: error("No queue named $name to connect to.")
    }

    override fun get(): Record {
        return q!!.first()
    }
}

class LocalOutputChannel(
    name: String,
    private val channelArgs: Map<ChannelArgs, Any>
) : OutputChannel(name) {
    private var queueDict: HashMap<String, Queue<Record>> =
        channelArgs[ChannelArgs.LOCAL_QUEUE_DICT] as HashMap<String, Queue<Record>>
    private var q: Queue<Record>? = null

    override fun connect() {
        q = queueDict[name] ?: error("No queue named $name to connect to.")
    }

    override fun put(record: Record) {
        q!!.add(record)
    }

    override fun flush() {
        // Do nothing
    }
}

