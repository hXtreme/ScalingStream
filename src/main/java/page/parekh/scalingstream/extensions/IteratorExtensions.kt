package page.parekh.scalingstream.extensions

fun <T> Iterator<T>.find(predicate: (T) -> Boolean) : T? {
    while (hasNext()) {
        val it = next()
        return if (predicate(it)) it else continue
    }
    return null
}

fun <T> Iterator<T>.take(n: Int): Sequence<T> {
    return sequence {
        for(i in 0 until n) {
            // Should have just the right amount of side effect on the iterator
            if (this@take.hasNext()) yield(this@take.next())
            else break
        }
    }
}

fun <T> Iterator<T>.first(): T {
    return next()
}
