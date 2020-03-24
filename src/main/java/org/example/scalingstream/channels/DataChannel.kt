package org.example.scalingstream.channels

abstract class DataChannel internal constructor(var name: String) {
    abstract fun connect()

}