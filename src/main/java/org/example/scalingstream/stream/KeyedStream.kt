package org.example.scalingstream.stream

import org.example.scalingstream.dag.Operator
import org.example.scalingstream.operator.Reduce
import org.example.scalingstream.partitioner.PartitionerConstructor

class KeyedStream<Incoming, Key, Outgoing>(
    operator: Operator<Incoming, *, *, Pair<Key, Outgoing>>,
    batchSize: Int,
    parallelism: Int,
    partitioner: PartitionerConstructor
) : Stream<Incoming, Pair<Key, Outgoing>>(operator, batchSize, parallelism, partitioner) {

    constructor(stream: Stream<Incoming, Pair<Key, Outgoing>>) : this(
        stream.operator,
        stream.batchSize,
        stream.parallelism,
        stream.partitioner
    )

    fun reduce(
        batchSize: Int = this.batchSize,
        parallelism: Int = this.parallelism,
        partitioner: PartitionerConstructor = this.partitioner,
        reduceFn: (Outgoing, Outgoing) -> Outgoing
    ): Stream<Pair<Key, Outgoing>, Outgoing> {
        return addOperator("reduce", ::Reduce, batchSize, parallelism, partitioner) { reduceFn(it.first, it.second) }
    }
}
