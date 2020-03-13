package org.example.scalingstream.executor

import org.example.scalingstream.stream.Node

interface Executor {
    val name: String
    val type: String

    /**
     * This will dispatch the Operator Managers according to policy and run them.
     */
    fun exec(dag: Set<Node<*, *, *, *>>): Unit
}