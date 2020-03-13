package org.example.scalingstream.stream

import org.example.scalingstream.operator.Reduce
import org.example.scalingstream.partitioner.Partitioner

class KeyedStream<Incoming, Key, Outgoing>(
    node: Node<Incoming, *, *, Pair<Key, Outgoing>>,
    batchSize: Int,
    parallelism: Int,
    partitioner: (Int) -> Partitioner
) : Stream<Incoming, Pair<Key, Outgoing>>(node, batchSize, parallelism, partitioner) {

    constructor(stream: Stream<Incoming, Pair<Key, Outgoing>>) : this(
        stream.node,
        stream.batchSize,
        stream.parallelism,
        stream.partitioner
    )

    fun reduce(
        batchSize: Int = this.batchSize,
        parallelism: Int = this.parallelism,
        partitioner: (Int) -> Partitioner = this.partitioner,
        reduceFn: (Outgoing, Outgoing) -> Outgoing
    ): Stream<Pair<Key, Outgoing>, Outgoing> {
        return addOperator("reduce", ::Reduce, batchSize, parallelism, partitioner) { reduceFn(it.first, it.second) }
    }
}