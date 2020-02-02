package org.example.scalingstream.partitioner;

import org.example.scalingstream.Record;
import org.example.scalingstream.operator.OutputBuffers;

public abstract class Partitioner {
    protected int numOut;

    Partitioner(int numOut) {
        this.numOut = numOut;
    }

    public abstract int assignPartition(Record record);


    public void assignPartition(OutputBuffers outputBuffers, Record[] records) {
        for (Record record: records) {
            outputBuffers.append(assignPartition(record), record);
        }
    }
}

