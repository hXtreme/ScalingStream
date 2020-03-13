package org.example.scalingstream.executor

import org.example.scalingstream.stream.Node


class LocalExecutor(override val name: String) : Executor {

    override val type: String = "LOCAL"

    override fun exec(dag: Set<Node<*, *, *, *>>) {

        val operatorThreads: List<Thread> = dag.map { Thread({it.run()}, it.operatorID) }

//        dag.forEach { operatorThreads.add(Thread({ it.run() }, it.operatorID)) }
        operatorThreads.forEach { it.start() }
        operatorThreads.forEach { it.join() }
    }
}
