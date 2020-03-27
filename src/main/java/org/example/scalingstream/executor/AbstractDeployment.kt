package org.example.scalingstream.executor

import org.example.scalingstream.operator.Task

abstract class AbstractDeployment(createTask: () -> Task<*, *, *, *>) : Deployment {

    override fun run() {
        error("Can't run an already deployed Task.")
    }
}
