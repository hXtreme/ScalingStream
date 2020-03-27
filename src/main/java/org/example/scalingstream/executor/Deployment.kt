package org.example.scalingstream.executor

import org.example.scalingstream.operator.Task


interface Deployment : Task<Any, Any, Any, Any> {
    /**
     * Has this Deployment finished execution?
     */
    val isDone: Boolean

    /**
     * Waits for the corresponding task to die.
     */
    fun join()
}
