package org.example.scalingstream.channels;

import org.example.scalingstream.Record;

public abstract class InputChannel extends DataChannel {

    public InputChannel(String name) {
        super(name);
    }

    abstract public Record get();
}

