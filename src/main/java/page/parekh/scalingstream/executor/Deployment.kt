package page.parekh.scalingstream.executor

import page.parekh.scalingstream.operator.Task


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
