package org.example.scalingstream.stream

import org.example.scalingstream.operator.*
import org.example.scalingstream.partitioner.Partitioner
import org.example.scalingstream.partitioner.RoundRobinPartitioner

open class Stream<Incoming, Outgoing>(
    val node: Node<Incoming, *, *, Outgoing>,
    val batchSize: Int = 1,
    val parallelism: Int = 1,
    val partitioner: (Int) -> Partitioner = ::RoundRobinPartitioner
) {
    protected fun <FnInp, FnOut, OutputType> addOperator(
        name: String,
        operatorConstructor: OperatorConstructor<Outgoing, FnInp, FnOut, OutputType>,
        batchSize: Int = this.batchSize,
        parallelism: Int = this.parallelism,
        partitioner: (Int) -> Partitioner = this.partitioner,
        operatorFn: (FnInp) -> FnOut
    ): Stream<Outgoing, OutputType> {
        val dagBuilder = node.addOperator(name, operatorConstructor, batchSize, parallelism, partitioner, operatorFn)

        return Stream(dagBuilder, batchSize, parallelism, partitioner)
    }

    protected fun <OutputType> addSimpleOperator(
        name: String,
        operatorConstructor: SimpleOperatorConstructor<Outgoing, OutputType>,
        batchSize: Int = this.batchSize,
        parallelism: Int = this.parallelism,
        partitioner: (Int) -> Partitioner = this.partitioner,
        operatorFn: (Outgoing) -> OutputType
    ): Stream<Outgoing, OutputType> {
        return addOperator(name, operatorConstructor, batchSize, parallelism, partitioner, operatorFn)
    }

    fun inspect(
        name: String = "sink",
        batchSize: Int = this.batchSize,
        parallelism: Int = this.parallelism,
        partitioner: (Int) -> Partitioner = this.partitioner,
        inspectOperator: (Outgoing) -> Unit
    ) {
        addSimpleOperator(name, ::Sink, batchSize, parallelism, partitioner, inspectOperator)
    }

    fun print(
        batchSize: Int = this.batchSize,
        parallelism: Int = this.parallelism,
        partitioner: (Int) -> Partitioner = this.partitioner
    ): Unit = inspect("print", batchSize, parallelism, partitioner) { print("$it\n") }

    fun drop(
        batchSize: Int = this.batchSize,
        parallelism: Int = this.parallelism,
        partitioner: (Int) -> Partitioner = this.partitioner
    ): Unit = inspect("drop", batchSize, parallelism, partitioner) { _ -> }

    fun <OutputType> map(
        batchSize: Int = this.batchSize,
        parallelism: Int = this.parallelism,
        partitioner: (Int) -> Partitioner = this.partitioner,
        mapFn: (Outgoing) -> OutputType
    ): Stream<Outgoing, OutputType> {
        return addSimpleOperator("map", ::Map, batchSize, parallelism, partitioner, mapFn)
    }

    fun <OutputType> flatMap(
        batchSize: Int = this.batchSize,
        parallelism: Int = this.parallelism,
        partitioner: (Int) -> Partitioner = this.partitioner,
        flatMapFn: (Outgoing) -> Iterable<OutputType>
    ): Stream<Outgoing, OutputType> {
        return addOperator("flatMap", ::FlatMap, batchSize, parallelism, partitioner, flatMapFn)
    }

    fun <KeyType> keyBy(
        batchSize: Int = this.batchSize,
        parallelism: Int = this.parallelism,
        partitioner: (Int) -> Partitioner = this.partitioner,
        selectorFn: (Outgoing) -> KeyType
    ): KeyedStream<Outgoing, KeyType, Outgoing> {
        return KeyedStream(addSimpleOperator("keyBy", ::Map, batchSize, parallelism, partitioner) {
            Pair(selectorFn(it), it)
        })
    }

    fun filter(
        batchSize: Int = this.batchSize,
        parallelism: Int = this.parallelism,
        partitioner: (Int) -> Partitioner = this.partitioner,
        filterFn: (Outgoing) -> Boolean
    ): Stream<Outgoing, Outgoing> {
        return addOperator("filter", ::Filter, batchSize, parallelism, partitioner, filterFn)
    }

}
