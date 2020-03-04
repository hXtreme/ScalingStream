package org.example.scalingstream.executor

import org.example.scalingstream.dag.DaG

interface Executor {
    val name: String
    val type: String

    fun exec(dag: DaG) : Unit
}