package org.example.scalingstream.channels;

public abstract class DataChannel {
  String name;

  DataChannel(String name) { this.name = name; }

  abstract public void connect();
}
