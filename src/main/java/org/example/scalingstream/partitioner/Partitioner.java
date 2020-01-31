package org.example.scalingstream.partitioner;

import org.example.scalingstream.Record;
import org.example.scalingstream.operator.OutputBuffer;

public abstract class Partitioner {
    protected int numOut;

    Partitioner(int numOut) {
        this.numOut = numOut;
    }

    public abstract int assignPartition(Record record);


    public void assignPartition(OutputBuffer outputBuffer, Record[] records) {
        for (Record record: records) {
            outputBuffer.append(assignPartition(record), record);
        }
    }
}

