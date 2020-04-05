package page.parekh.scalingstream.partitioner


class HashPartitioner : Partitioner {
    override val name: String = "HASH"

    override fun <Type> assignPartition(record: Type, numOut: Int): Int {
        if (numOut == 0) error("Can't partition into zero partitions.")
        return record.hashCode() % numOut
    }
}
