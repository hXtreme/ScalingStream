package org.example.scalingstream.operator

import de.jupf.staticlog.Log
import org.example.scalingstream.channels.ChannelBuilder
import org.example.scalingstream.control.InputChannelManager
import org.example.scalingstream.control.OutputChannelManager
import org.example.scalingstream.partitioner.Partitioner


typealias TaskConstructor<InputType, FnInp, FnOut, OutputType> =
            (Int, String, List<String>, Int, ChannelBuilder, Int, Partitioner, (FnInp) -> FnOut)
        -> Task<InputType, FnInp, FnOut, OutputType>

typealias SimpleTask<InputType, OutputType> =
        Task<InputType, InputType, OutputType, OutputType>

typealias SimpleTaskConstructor<InputType, OutputType> =
        TaskConstructor<InputType, InputType, OutputType, OutputType>

abstract class Task<InputType, FnInp, FnOut, OutputType>(
    protected val taskID: Int,
    protected val operatorID: String,
    protected val inputChannelManagerList: List<InputChannelManager<InputType>>,
    protected val outputChannelManagerList: List<OutputChannelManager<OutputType>>,
    protected val operatorFn: (FnInp) -> FnOut
) {
    protected val tag = "$operatorID$taskID"
    init {
        Log.info("$inputChannelManagerList\t-->\t$operatorID\t-->\t$outputChannelManagerList", tag)
    }

    abstract fun run(): Any

    protected abstract fun processRecord(record: InputType): OutputType

    protected open fun processBatch(batch: List<InputType>) {
        val processed = batch.map { record -> processRecord(record) }
        outputChannelManagerList.forEach { it.put(processed) }
        numProduced += processed.size
    }

    protected val inputChannelManagers = sequence {
        var current = 0
        while (true) {
            current %= inputChannelManagerList.size
            yield(inputChannelManagerList[current])
            current++
        }
    }

    var numConsumed: Int = 0
        protected set

    var numProduced: Int = 0
        protected set

    override fun toString(): String {
        return "${operatorID}${taskID}"
    }
}
