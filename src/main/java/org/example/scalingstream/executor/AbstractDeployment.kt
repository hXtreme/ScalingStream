package org.example.scalingstream.executor

import org.example.scalingstream.operator.Task

abstract class AbstractDeployment<InputType, FnInp, FnOut, OutputType>(task: Task<InputType, FnInp, FnOut, OutputType>) :
    Deployment<InputType, FnInp, FnOut, OutputType> {

    override fun run() {
        error("Can't run an already deployed Task.")
    }
}