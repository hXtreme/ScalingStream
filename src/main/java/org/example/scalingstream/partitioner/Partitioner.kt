package org.example.scalingstream.partitioner

import org.example.scalingstream.operator.OutputBuffers

typealias PartitionerConstructor = () -> Partitioner

abstract class Partitioner {

    abstract fun <Type> assignPartition(record: Type, numOut: Int): Int

    open fun <Type> partitionBatch(
        outputBuffers: OutputBuffers<Type>,
        recordBatch: List<Type>
    ) {
        val numOut = outputBuffers.numOut
        if (numOut <= 0) error("Can't partition to zero partitions")
        for (record in recordBatch) {
            outputBuffers.append(assignPartition(record, numOut), record)
        }
    }

}
