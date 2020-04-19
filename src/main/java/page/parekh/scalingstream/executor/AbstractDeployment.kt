package page.parekh.scalingstream.executor

abstract class AbstractDeployment : Deployment {

    override fun run() {
        error("Can't run an already deployed Task.")
    }
}
