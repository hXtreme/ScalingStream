package page.parekh.scalingstream.operator

import page.parekh.scalingstream.control.channel.ChannelReaderManager
import page.parekh.scalingstream.control.channel.ChannelWriterManager
import java.util.*

class FlatMap<InputType, OutputType>(
    taskID: UUID,
    operatorID: String,
    channelReaderManagerList: List<ChannelReaderManager<InputType>>,
    channelWriterManagerList: List<ChannelWriterManager<OutputType>>,
    operatorFn: (InputType) -> Iterable<OutputType>
) : SingleInputTask<InputType, InputType, Iterable<OutputType>, OutputType>(
    taskID,
    operatorID,
    channelReaderManagerList,
    channelWriterManagerList,
    operatorFn
) {
    override fun processBatch(batch: List<InputType>) {
        val processed = batch.flatMap { it -> operatorFn(it) }
        channelWriterManagerList.forEach { it.put(processed) }
        numProduced += processed.size
    }

    override fun processRecord(record: InputType): OutputType {
        error("Unused function, should not have been called.")
    }
}
