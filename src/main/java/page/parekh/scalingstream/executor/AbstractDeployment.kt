package page.parekh.scalingstream.executor

import page.parekh.scalingstream.operator.Task

abstract class AbstractDeployment(createTask: () -> Task<*, *, *, *>) : Deployment {

    override fun run() {
        error("Can't run an already deployed Task.")
    }
}
