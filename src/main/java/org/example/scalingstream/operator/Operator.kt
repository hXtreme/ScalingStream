package org.example.scalingstream.operator

import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner


abstract class Operator internal constructor(
    protected val idx: Int,
    protected val operatorID: String,
    protected val outOperatorIDs: List<String>,
    protected val operatorFn: (Any) -> Any, // TODO: Fix the type of operatorFn
    protected val partitioner: Partitioner,
    protected val upstreamCount: Int,
    protected val batchSize: Int,
    protected val channelBuilder: ChannelBuilder
) {
    protected val numOut: Int = outOperatorIDs.size
    protected var numProcessed : Int = 0

    abstract fun run() : Unit
}

