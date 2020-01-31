package org.example.scalingstream.channels

import org.example.scalingstream.Record

import java.util.Queue
import java.util.ArrayDeque
import kotlin.collections.HashMap


class LocalChannelContext(
    name: String,
    private val queue_dict: HashMap<String, Queue<Record>>,
    private val maxQueueLength: Int
) : DataChannelContext(name) {

    override fun init() {
        // TODO("Add size restriction")
        queue_dict[name] = ArrayDeque<Record>()
    }

    override fun destroy() {
        queue_dict.remove(name)
    }
}

class LocalInputChannel(
    name: String,
    private val queue_dict: HashMap<String, Queue<Record>>
) : InputChannel(name) {
    private var q: Queue<Record>? = null

    override fun connect() {
        q = queue_dict[name] ?: error("No queue named $name to connect to.")
    }

    override fun get(): Record {
        return q!!.first()
    }
}

class LocalOutputChannel(
    name: String,
    private val queue_dict: HashMap<String, Queue<Record>>
) : OutputChannel(name) {
    private var q: Queue<Record>? = null

    override fun connect() {
        q = queue_dict[name] ?: error("No queue named $name to connect to.")
    }

    override fun put(record: Record) {
        q!!.add(record)
    }

    override fun flush() {
        // Do nothing
    }
}

