package org.example.scalingstream.operator

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.*

internal class SourceTest {

    @Test
    fun buildSource() {
        @SuppressWarnings("unused")
        val s = Source(
            UUID.randomUUID(),
            "source",
            emptyList(),
            emptyList(),
            (fun(_): Any { return 1 })
        )

        assertTrue(true)
    }

    @Test
    fun run() {
        val source = Source(
            UUID.randomUUID(),
            "source",
            emptyList(),
            emptyList(),
            fun(_): Any { return 1 }
        )

        source.run()
        assertTrue(true)
    }
}
