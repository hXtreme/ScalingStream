package org.example.scalingstream.channels;

public abstract class DataChannelContext {
    String name;

    DataChannelContext(String name){
        this.name = name;
    }

    abstract public void init();

    abstract public void destroy();
}
