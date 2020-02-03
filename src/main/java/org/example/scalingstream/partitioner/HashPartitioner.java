package org.example.scalingstream.partitioner;

import org.example.scalingstream.Record;

public class HashPartitioner extends Partitioner {
    HashPartitioner(int numOut) {
        super(numOut);
    }

    @Override
    public int assignPartition(Record record) {
        return record.hashCode();
    }
}
