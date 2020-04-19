package page.parekh.scalingstream.partitioner

import kotlin.math.abs


class HashPartitioner : Partitioner {
    override val name: String = "HASH"

    override fun <Type> assignPartition(record: Type, numOut: Int): Int {
        if (numOut == 0) error("Can't partition into zero partitions.")
        return abs(record.hashCode() % numOut)
    }
}
