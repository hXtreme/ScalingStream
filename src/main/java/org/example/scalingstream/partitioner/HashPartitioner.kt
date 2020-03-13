package org.example.scalingstream.partitioner


class HashPartitioner(numOut: Int) : Partitioner(numOut) {
    override fun <Type> assignPartition(record: Type): Int {
        return record.hashCode() % numOut
    }
}
