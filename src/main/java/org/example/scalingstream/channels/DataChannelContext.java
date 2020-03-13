package org.example.scalingstream.channels;

import java.util.Map;

public abstract class DataChannelContext {
  String name;
  Map<ChannelArg, Object> channelArgs;

  DataChannelContext(String name) { this.name = name; }

  DataChannelContext(String name, Map<ChannelArg, Object> channelArgs) {
    this(name);
    this.channelArgs = channelArgs;
  }

  abstract public void destroy();
}
