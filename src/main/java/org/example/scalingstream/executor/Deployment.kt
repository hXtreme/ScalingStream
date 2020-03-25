package org.example.scalingstream.executor

import org.example.scalingstream.operator.Task

typealias ObliviousDeployment = Deployment<*, *, *, *>

interface Deployment<InputType, FnInp, FnOut, OutputType> : Task<InputType, FnInp, FnOut, OutputType> {
    /**
     * Has this Deployment finished execution?
     */
    val isDone: Boolean

    /**
     * Waits for the corresponding task to die.
     */
    fun join()
}