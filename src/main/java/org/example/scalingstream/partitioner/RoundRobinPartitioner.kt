package org.example.scalingstream.partitioner

class RoundRobinPartitioner : Partitioner() {
    private var i = 0
    override fun <Type> assignPartition(record: Type, numOut: Int): Int {
        i = ++i % numOut
        return i
    }
}
