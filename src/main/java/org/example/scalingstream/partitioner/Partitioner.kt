package org.example.scalingstream.partitioner

import org.example.scalingstream.operator.OutputBuffers

abstract class Partitioner internal constructor(protected val numOut: Int) {

    abstract fun <Type> assignPartition(record: Type): Int

    open fun <Type> partitionBatch(
        outputBuffers: OutputBuffers<Type>,
        recordBatch: List<Type>
    ) {
        if (numOut <= 0) error("Can't partition to zero partitions")
        for (record in recordBatch) {
             outputBuffers.append(assignPartition(record), record)
        }
    }

}