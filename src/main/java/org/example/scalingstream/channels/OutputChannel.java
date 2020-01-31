package org.example.scalingstream.channels;

import org.example.scalingstream.Record;

public abstract class OutputChannel extends DataChannel {

    public OutputChannel(String name) {
        super(name);
    }

    abstract public void put(Record record);

    abstract public void flush();
}
