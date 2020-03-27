package org.example.scalingstream.channels.jiffy

import org.example.scalingstream.channels.*
import java.time.Instant

class JiffyChannelWriter<Type>(
    name: ChannelID, channelArgs: ChannelArgs
) : ChannelWriter<Type>(name) {
    private val path = "/$name"

    private val host: String = channelArgs.getOrDefault(ChannelArg.JIFFY_HOST, "127.0.0.1") as String
    private val servicePort: Int = channelArgs.getOrDefault(ChannelArg.JIFFY_SERVICE_PORT, 9090) as Int
    private val leasePort: Int = channelArgs.getOrDefault(ChannelArg.JIFFY_LEASE_PORT, 9091) as Int

    override fun connect() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun put(recordBatch: Record<Type>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun flush() {
        // Do nothing
    }

    override fun close(timestamp: Instant?) {
        TODO("Not yet implemented")
    }
}
