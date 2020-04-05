package page.parekh.scalingstream.channels.local

import de.jupf.staticlog.Log
import java.util.concurrent.LinkedBlockingQueue

class CloseableLinkedBlockingQueue<Type>(capacity: Int) : LinkedBlockingQueue<Type>(capacity) {
    var isClosed: Boolean = false
        private set

    fun close() {
        Log.debug("Queue closed.", this.javaClass.name)
        isClosed = true
    }

    override fun put(e: Type) {
        if (isClosed) error("Can't put into a closed Queue.")
        super.put(e)
    }
}
