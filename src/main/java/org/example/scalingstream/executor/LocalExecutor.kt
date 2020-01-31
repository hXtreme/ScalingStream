package org.example.scalingstream.executor

import org.example.scalingstream.dag.DAG


class LocalExecutor : Executor {
    override fun exec(dag: DAG) {
        val threads = ArrayList<Thread>()
        val stagesCount = dag.size
        for (i in 0 until stagesCount) {
            val stage = dag.popFirst();
            for (operator in stage) {
                val thread = Thread(operator)
                threads.add(thread)
                thread.start()
            }
        }

        for (thread in threads) {
            thread.join()
        }
    }
}
