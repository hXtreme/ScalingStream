package page.parekh.scalingstream.operator

import de.jupf.staticlog.Log
import page.parekh.scalingstream.control.channel.ChannelReaderManager
import page.parekh.scalingstream.control.channel.ChannelWriterManager
import java.util.*


typealias TaskConstructor<InputType, FnInp, FnOut, OutputType> =
            (UUID, String, List<ChannelReaderManager<InputType>>, List<ChannelWriterManager<OutputType>>, (FnInp) -> FnOut)
        -> Task<InputType, FnInp, FnOut, OutputType>

typealias SimpleTaskConstructor<InputType, OutputType> =
        TaskConstructor<InputType, InputType, OutputType, OutputType>

abstract class AbstractTask<InputType, FnInp, FnOut, OutputType>(
    override val taskID: UUID,
    override val operatorID: String,
    val channelReaderManagerList: List<ChannelReaderManager<InputType>>,
    val channelWriterManagerList: List<ChannelWriterManager<OutputType>>,
    protected val operatorFn: (FnInp) -> FnOut
) : Task<InputType, FnInp, FnOut, OutputType> {
    override var numConsumed: Int = 0
        protected set

    override var numProduced: Int = 0
        protected set

    init {
        Log.info("$channelReaderManagerList\t-->\t$name\t-->\t$channelWriterManagerList", name)
    }

    abstract override fun run()

    protected abstract fun processRecord(record: InputType): OutputType

    protected open fun processBatch(batch: List<InputType>) {
        val processed = batch.map { record -> processRecord(record) }
        channelWriterManagerList.forEach { it.put(processed) }
        numProduced += processed.size
    }

    protected val inputChannelManagers = iterator {
        var current = 0
        while (true) {
            current %= channelReaderManagerList.size
            yield(channelReaderManagerList[current])
            current++
        }
    }

    override fun toString(): String {
        return name
    }
}
