package org.example.scalingstream.operator

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.*

internal class SourceTest {

    private fun count(end: Int): Iterator<Int?> {
        val iter = iterator<Int?> {
            var n = 0
            while (n <= end) {
                yield(n++)
            }
            yield(null)
        }
        return iter
    }

    @Test
    fun buildSource() {
        val iter = count(10)
        @SuppressWarnings("unused")
        val s = Source(
            UUID.randomUUID(),
            "source",
            emptyList(),
            emptyList()
        ) { iter.next() }

        assertTrue(true)
    }

    @Test
    fun run() {
        val iter = count(10)
        val source = Source(
            UUID.randomUUID(),
            "source",
            emptyList(),
            emptyList()
        ) { iter.next() }

        source.run()
        assertTrue(true)
    }
}
