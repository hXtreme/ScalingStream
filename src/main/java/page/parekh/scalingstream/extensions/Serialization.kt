package page.parekh.scalingstream.extensions

import org.apache.avro.util.ByteBufferInputStream
import java.io.*
import java.nio.ByteBuffer

fun <T : Serializable> serialize(obj: T): ByteBuffer {
    val byteStream = ByteArrayOutputStream()
    ObjectOutputStream(byteStream).writeObject(obj)
    return ByteBuffer.wrap(byteStream.toByteArray())
}

fun <T : Serializable> deserialize(byteBuff: ByteBuffer): T {
    val byteStream = ByteBufferInputStream(listOf(byteBuff))
    return ObjectInputStream(byteStream).readObject() as? T
        ?: error("Serialized Object could not be cast into the given type.")
}
