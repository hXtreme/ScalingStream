package org.example.scalingstream.partitioner

typealias PartitionerConstructor = () -> Partitioner

interface Partitioner {
    val name: String

    fun <Type> assignPartition(record: Type, numOut: Int): Int

}
