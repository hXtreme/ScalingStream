package org.example.scalingstream.operator

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.*

internal class SinkTest {

    @Test
    fun buildSink() {
        @SuppressWarnings("unused")
        val sink = Sink(
            UUID.randomUUID(),
            "0",
            emptyList(),
            emptyList(),
            fun(_: Unit) {}
        )

        assertTrue(true)
    }

    @Test
    fun run() {
        val sink = Sink(
            UUID.randomUUID(),
            "0",
            emptyList(),
            emptyList(),
            fun(_: Unit) {}
        )

        sink.run()
        assertTrue(true)
    }
}
