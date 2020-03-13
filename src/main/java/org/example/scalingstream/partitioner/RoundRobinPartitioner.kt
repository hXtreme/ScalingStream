package org.example.scalingstream.partitioner

class RoundRobinPartitioner(numOut: Int) : Partitioner(numOut) {
    private var i = 0
    override fun <Type> assignPartition(record: Type): Int {
        i = ++i % numOut
        return i
    }
}
