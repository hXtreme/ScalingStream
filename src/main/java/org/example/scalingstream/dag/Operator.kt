package org.example.scalingstream.dag

import de.jupf.staticlog.Log
import org.example.scalingstream.channels.Channel
import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.control.channel.ChannelReaderManagerImpl
import org.example.scalingstream.control.channel.BufferedChannelWriterManager
import org.example.scalingstream.executor.DeployFn
import org.example.scalingstream.executor.Deployment
import org.example.scalingstream.operator.TaskConstructor
import org.example.scalingstream.partitioner.PartitionerConstructor
import org.example.scalingstream.stream.ChannelManager
import org.example.scalingstream.stream.StreamBuilder
import java.util.*
import kotlin.collections.HashMap

class Operator<InputType, FnIn, FnOut, OutputType>(
    name: String,
    private val streamBuilder: StreamBuilder,
    private val taskConstructor: TaskConstructor<InputType, FnIn, FnOut, OutputType>,
    val batchSize: Int,
    private val initialParallelism: Int = 1,
    val partitioner: PartitionerConstructor,
    private val operatorFn: (FnIn) -> FnOut
) {
    val name = "${name}_${getUID(name)}"

    private val graph = streamBuilder.streamExecutionDAG

    val deployedTasks: MutableMap<UUID, Deployment> = HashMap()

    var isRunning: Boolean = false
        private set

    fun run(deployFn: DeployFn) {
        Log.debug("Running operator.", name)
        isRunning = true
        spawnTasks(initialParallelism, deployFn)
    }

    fun spawnTasks(count: Int, deployFn: DeployFn): List<UUID> {
        if (!isRunning) error("Can't spawn tasks when operator($name) is not running.")
        val ids = List(count) { UUID.randomUUID() }
        ids.forEach { taskID -> spawnTask(deployFn, taskID) }
        return ids
    }

    private fun spawnTask(deployFn: DeployFn, id: UUID) {
        // Setup the output channels - read-endpoint and write-endpoint
        val channelWriteManagers = outgoingChannelManagers
            .map { channelManager ->
                val dst = channelManager.dst
                // Build all the outgoing channels
                val channels = dst.deployedTasks.keys.map { destID ->
                    channelManager.channelBuilder(ChannelID(id, destID))
                }
                // Add the read-endpoint of the channels
                channels.forEach { channel ->
                    channelManager.addChannelReader(channel.id)
                }

                // Add the write-endpoint to a new OutputChannelManager
                val channelWriteManager = BufferedChannelWriterManager<OutputType>(batchSize, partitioner)
                channelManager.registerChannelWriteManager(id, channelWriteManager)
                channels
                    .forEach { channel ->
                        channelManager.addChannelWriter(channel.id)
                        // channel.getChannelWriter()
                    }
                //.forEach { outputChannel -> channelWriteManager.addChannel(outputChannel) }

                // Register and return the OutputChannelManager
                channelWriteManager
            }

        // Setup the input channels - read-endpoint
        val inputChannels: MutableSet<Pair<ChannelManager<InputType>,List<Channel<InputType>>>> =
            mutableSetOf()
        val inputChannelManagers = incomingChannelManagers
            .map { channelManager ->
                val src = channelManager.src
                // Build all the incoming channels
                val channels = src.deployedTasks.keys.map { srcID ->
                    channelManager.channelBuilder(ChannelID(srcID, id))
                }

                inputChannels.add(Pair(channelManager, channels))

                // Add the read-endpoint to a new InputChannelManager
                val channelReadManager = ChannelReaderManagerImpl<InputType>()
                channelManager.registerChannelReadManager(id, channelReadManager)
                channels
                    .forEach { channel ->
                        channelManager.addChannelReader(channel.id)
                        //channel.getChannelReader()
                    }
                // Register and return the OutputChannelManager
                channelReadManager
            }

        // Setup and start the task.
        val deployment =
            deployFn(this) { taskConstructor(id, name, inputChannelManagers, channelWriteManagers, operatorFn) }

        // Add the write-endpoint of the input channels
        inputChannels.forEach { (channelManager, channelList) ->
            channelList.forEach { channel ->
                channelManager.addChannelWriter(channel.id)
//                channel.getChannelWriter()
            }
        }
        deployedTasks[id] = deployment
    }

    fun removeTasks(taskIDs: Collection<UUID>) {
        if (!isRunning) error("Can't kill tasks when operator($name) is not running.")
        deployedTasks.filterKeys { it in taskIDs }.keys.forEach { removeTask(it) }
    }

    fun removeTask(taskID: UUID) {
        if (!isRunning) error("Can't remove task($taskID) when operator($name) is not running.")
        incomingChannelManagers.forEach { channelManager ->
            val srcs = channelManager.src.deployedTasks.keys
            srcs.map { src -> ChannelID(src, taskID) }.forEach { channelManager.destroy(it) }
        }
        // deployedTasks[taskID].kill()
        deployedTasks.remove(taskID)
        // TODO("Not yet implemented")
    }

    fun <FnIn, FnOut, Type> addOperator(
        name: String,
        task: TaskConstructor<OutputType, FnIn, FnOut, Type>,
        batchSize: Int,
        initialParallelism: Int,
        partitioner: PartitionerConstructor,
        operatorFn: (FnIn) -> FnOut
    ): Operator<OutputType, FnIn, FnOut, Type> {
        return streamBuilder.addOperator(this, name, task, batchSize, initialParallelism, partitioner, operatorFn)
    }

    val taskIDs: Set<UUID>
        get() {
            return deployedTasks.keys
        }

    var parallelism: Int = initialParallelism
        get() {
            return deployedTasks.size
        }
        private set

    private val outgoingChannelManagers: Set<ChannelManager<OutputType>>
        get() {
            return graph.outgoingEdgesOf(this) as Set<ChannelManager<OutputType>>
        }

    private val incomingChannelManagers: Set<ChannelManager<InputType>>
        get() {
            return graph.incomingEdgesOf(this) as Set<ChannelManager<InputType>>
        }

    companion object StaticVar {
        var uID: MutableMap<String, Int> = HashMap()

        fun getUID(name: String): Int {
            val id = uID.getOrPut(name, { 0 })
            uID.computeIfPresent(name) { _, v -> v + 1 }
            return id
        }
    }

    override fun toString(): String {
        return name
    }

    override fun equals(other: Any?): Boolean {
        return other is Operator<*, *, *, *> && toString() == other.toString()
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }
}