package org.example.scalingstream.partitioner;

import org.example.scalingstream.Record;
import org.example.scalingstream.operator.OutputBuffer;

public class RoundRobinPartitioner extends Partitioner {
    private int i = 0;

    RoundRobinPartitioner(int numOut) {
        super(numOut);
        this.i = 0;
    }

    @Override
    public int assignPartition(Record record) {
        return (i++) % numOut;
    }

    @Override
    public void assignPartition(OutputBuffer outputBuffer, Record[] records) {
        for (Record record: records) {
            outputBuffer.append(assignPartition(record), record);
        }
    }
}
