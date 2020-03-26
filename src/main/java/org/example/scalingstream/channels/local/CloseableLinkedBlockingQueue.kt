package org.example.scalingstream.channels.local

import java.util.concurrent.LinkedBlockingQueue

class CloseableLinkedBlockingQueue<Type>(capacity: Int) : LinkedBlockingQueue<Type>(capacity) {
    var isClosed: Boolean = false
        private set

    var isClosedAndEmpty: Boolean = false
        get() {
            return isClosed and this.isEmpty()
        }
        private set

    override fun put(e: Type?) {
        if (e == null) isClosed = true
        else super.put(e)
    }
}