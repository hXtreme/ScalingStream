package org.example.scalingstream.channels

import java.time.Instant


object JiffyChannelConstants {
    const val TYPE = "JIFFY"
}

class JiffyChannelBuilder(
    override val name: String,
    override val channelArgs: ChannelArgs
) : ChannelBuilder {
    override val type: String = JiffyChannelConstants.TYPE

    private val host: String = channelArgs.getOrDefault(ChannelArg.JIFFY_HOST, "127.0.0.1") as String
    private val servicePort: Int = channelArgs.getOrDefault(ChannelArg.JIFFY_SERVICE_PORT, 9090) as Int
    private val leasePort: Int = channelArgs.getOrDefault(ChannelArg.JIFFY_LEASE_PORT, 9091) as Int

    override fun <Type> buildChannelContext(name: String): DataChannelContext {
        return JiffyChannelContext(name, host, servicePort, leasePort)
    }

    override fun <Type> buildInputChannel(name: String): InputChannel<Type> {
        return JiffyInputChannel(name, host, servicePort, leasePort)
    }

    override fun <Type> buildOutputChannel(name: String): OutputChannel<Type> {
        return JiffyOutputChannel(name, host, servicePort, leasePort)
    }
}

class JiffyChannelContext(
    name: String,
    private val host: String = "127.0.0.1",
    private val servicePort: Int = 9090,
    private val leasePort: Int = 9091
) : DataChannelContext(name) {
    private val path = "/$name"

    init {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun destroy() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}


class JiffyInputChannel<Type>(
    name: String,
    private val host: String = "127.0.0.1",
    private val servicePort: Int = 9090,
    private val leasePort: Int = 9091
) : InputChannel<Type>(name) {
    private val path = "/$name"

    override fun connect() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun peek(): Pair<Instant?, List<Type>?>? {
        TODO("Not yet implemented")
    }

    override fun get(): Pair<Instant?, List<Type>?> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}


class JiffyOutputChannel<Type>(
    name: String,
    private val host: String = "127.0.0.1",
    private val servicePort: Int = 9090,
    private val leasePort: Int = 9091
) : OutputChannel<Type>(name) {
    private val path = "/$name"

    override fun connect() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun put(recordBatch: Pair<Instant?, List<Type>?>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun flush() {
        // Do nothing
    }
}


