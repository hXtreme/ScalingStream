package org.example.scalingstream.operator

import org.example.scalingstream.CONSTANTS
import org.example.scalingstream.Record
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.partitioner.Partitioner


abstract class Operator internal constructor(
    protected val idx: Int,
    protected val operatorID: String,
    protected val outOperatorIDs: List<String>,
    _operatorFn: (Any) -> Array<Any>, // TODO: Fix the type of operatorFn
    protected val partitioner: Partitioner,
    protected val upstreamCount: Int,
    protected val batchSize: Int,
    protected val channelBuilder: ChannelBuilder
) {
    protected val numOut: Int = outOperatorIDs.size
    protected var numProcessed : Int = 0

    protected val operatorFn = object : Iterable<Array<Record>> {
        override fun iterator(): Iterator<Array<Record>> {
            return object : Iterator<Array<Record>> {
                var nextItem = _operatorFn(batchSize)
                override fun hasNext(): Boolean {
                    return nextItem != CONSTANTS.DONE_MARKER
                }

                override fun next(): Array<Record> {
                    if (!hasNext()) throw NoSuchElementException("No items remaining.")
                    val item = nextItem
                    nextItem = _operatorFn(batchSize)
                    return item as Array<Record>
                }
            }
        }
    }

    abstract fun run() : Unit
}

