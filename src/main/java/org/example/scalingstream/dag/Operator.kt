package org.example.scalingstream.dag

import de.jupf.staticlog.Log
import org.example.scalingstream.channels.Channel
import org.example.scalingstream.channels.ChannelID
import org.example.scalingstream.control.channel.ChannelReadManagerImpl
import org.example.scalingstream.control.channel.ChannelWriteManagerImpl
import org.example.scalingstream.executor.DeployFn
import org.example.scalingstream.executor.ObliviousDeployment
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

    val deployedTasks: MutableMap<UUID, ObliviousDeployment> = HashMap()

    var parallelism = initialParallelism
        get() = deployedTasks.size
        private set

    var isRunning: Boolean = false
        private set

    val taskIDs
        get() = deployedTasks.keys

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
        val outgoingChannelManagers = graph.outgoingEdgesOf(this) as Set<ChannelManager<OutputType>>
        val channelWriteManagers = outgoingChannelManagers
            .map { channelManager ->
                val dst = channelManager.dst
                // Build all the outgoing channels
                val channels = dst.deployedTasks.keys.map { destID ->
                    val channelID = ChannelID(id, destID)
                    val channel = channelManager.channelBuilder(channelID)
                    Pair(channelID, channel)
                }
                // Add the read-endpoint of the channels
                channels.forEach { (channelID, channel) ->
                    val inputChannel = channel.getChannelReader()
                    channelManager.addChannelRead(channelID, inputChannel)
//                    dst.addInputChannel(channelID.second, inputChannel)
                }

                // Add the write-endpoint to a new OutputChannelManager
                val channelWriteManager = ChannelWriteManagerImpl<OutputType>(batchSize, partitioner)
                channels
                    .map { (_, channel) -> channel.getChannelWriter() }
                    .forEach { outputChannel -> channelWriteManager.addChannel(outputChannel) }

                // Register and return the OutputChannelManager
                channelManager.registerChannelWriteManager(id, channelWriteManager)
                channelWriteManager
            }

        // Setup the input channels - read-endpoint
        val incomingChannelManagers = graph.incomingEdgesOf(this) as Set<ChannelManager<InputType>>
        val inputChannels: MutableSet<Pair<ChannelManager<InputType>, List<Pair<ChannelID, Channel<InputType>>>>> =
            mutableSetOf()
        val inputChannelManagers = incomingChannelManagers
            .map { channelManager ->
                val src = channelManager.src
                // Build all the incoming channels
                val channels = src.deployedTasks.keys.map { srcID ->
                    val channelID = ChannelID(srcID, id)
                    val channel = channelManager.channelBuilder(channelID)
                    Pair(channelID, channel)
                }

                inputChannels.add(Pair(channelManager, channels))

                // Add the read-endpoint to a new InputChannelManager
                val channelReadManager = ChannelReadManagerImpl<InputType>()
                channels
                    .map { (_, channel) -> channel.getChannelReader() }
                    .forEach { inputChannel -> channelReadManager.addChannel(inputChannel) }

                // Register and return the OutputChannelManager
                channelManager.registerChannelReadManager(id, channelReadManager)
                channelReadManager
            }

        // Setup and start the task.
        val task = taskConstructor(id, name, inputChannelManagers, channelWriteManagers, operatorFn)
//        val thread = Thread({ task.run() }, task.name)
        val deployment = deployFn(this, task)

        // Add the write-endpoint of the input channels
        inputChannels.flatMap { (manager, channelList) ->
                channelList.map { (channelID, channel) ->
                    Triple(
                        manager,
                        channelID,
                        channel
                    )
                }
            }
            .forEach { (manager, channelID, channel) ->
                manager.addChannelWrite(channelID, channel.getChannelWriter())
            }
        deployedTasks[id] = deployment
    }

    fun removeTasks(taskIDs: Collection<UUID>) {
        if (!isRunning) error("Can't kill tasks when operator($name) is not running.")
        deployedTasks.filterKeys { it in taskIDs }.keys.forEach { removeTask(it) }
    }

    fun removeTask(taskID: UUID) {
        if (!isRunning) error("Can't spawn task($taskID) when operator($name) is not running.")
        TODO("Not yet implemented")
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