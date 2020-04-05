package page.parekh.scalingstream.operator

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.*

internal class SinkTest {

    @Test
    fun buildSink() {
        @SuppressWarnings("unused")
        val sink = Sink(
            UUID.randomUUID(),
            "sink",
            emptyList(),
            emptyList()
        ) { it: Int -> print(it) }

        assertTrue(true)
    }

    @Test
    fun run() {
        val sink = Sink(
            UUID.randomUUID(),
            "sink",
            emptyList(),
            emptyList()
        ) { it: Int -> print(it) }

        sink.run()
        assertTrue(true)
    }
}
