package org.example.scalingstream.channels;

import java.util.Map;

public abstract class DataChannelContext {
    String name;

    DataChannelContext(String name){
        this.name = name;
    }

    DataChannelContext(String name, Map<String, Object> channelArgs) {
        this(name);
    }

    abstract public void init();

    abstract public void destroy();
}
