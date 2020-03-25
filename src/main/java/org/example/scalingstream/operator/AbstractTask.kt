package org.example.scalingstream.operator

import de.jupf.staticlog.Log
import org.example.scalingstream.control.channel.ChannelReadManager
import org.example.scalingstream.control.channel.ChannelWriteManager
import java.util.*


typealias TaskConstructor<InputType, FnInp, FnOut, OutputType> =
            (UUID, String, List<ChannelReadManager<InputType>>, List<ChannelWriteManager<OutputType>>, (FnInp) -> FnOut)
        -> Task<InputType, FnInp, FnOut, OutputType>

typealias SimpleTaskConstructor<InputType, OutputType> =
        TaskConstructor<InputType, InputType, OutputType, OutputType>

abstract class AbstractTask<InputType, FnInp, FnOut, OutputType>(
    override val taskID: UUID,
    override val operatorID: String,
    val channelReadManagerList: List<ChannelReadManager<InputType>>,
    val channelWriteManagerList: List<ChannelWriteManager<OutputType>>,
    protected val operatorFn: (FnInp) -> FnOut
) : Task<InputType, FnInp, FnOut, OutputType> {
    override var numConsumed: Int = 0
        protected set

    override var numProduced: Int = 0
        protected set

    init {
        Log.info("$channelReadManagerList\t-->\t$name\t-->\t$channelWriteManagerList", name)
    }

    abstract override fun run()

    protected abstract fun processRecord(record: InputType): OutputType

    protected open fun processBatch(batch: List<InputType>) {
        val processed = batch.map { record -> processRecord(record) }
        channelWriteManagerList.forEach { it.put(processed) }
        numProduced += processed.size
    }

    protected val inputChannelManagers = iterator {
        var current = 0
        while (true) {
            current %= channelReadManagerList.size
            yield(channelReadManagerList[current])
            current++
        }
    }

    override fun toString(): String {
        return "${operatorID}${taskID.toString().substring(0, 10)}"
    }
}
