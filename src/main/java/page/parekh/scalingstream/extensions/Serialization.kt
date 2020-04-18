package page.parekh.scalingstream.extensions

import org.apache.avro.util.ByteBufferInputStream
import java.io.*
import java.nio.ByteBuffer
import java.util.*

fun Serializable.serialize() : ByteArray {
    val byteStream = ByteArrayOutputStream()
    ObjectOutputStream(byteStream).writeObject(this)
    return byteStream.toByteArray()
}

fun <T: Serializable> deserialize(byteArray: ByteArray): T {
    return ObjectInputStream(ByteArrayInputStream(byteArray)).readObject() as? T
        ?: error("Serialized Object could not be cast into the specified type.")
}

fun Serializable.serializeToString(): String {
    return Base64.getEncoder().encodeToString(this.serialize())
}

fun <T: Serializable> deserializeFromString(string: String): T {
    return deserialize(Base64.getDecoder().decode(string))
}

fun Serializable.serializeToByteBuffer(): ByteBuffer {
    return ByteBuffer.wrap(this.serialize())
}

fun <T : Serializable> deserializeFromByteBuffer(byteBuff: ByteBuffer): T {
    val byteStream = ByteBufferInputStream(listOf(byteBuff))
    return ObjectInputStream(byteStream).readObject() as? T
        ?: error("Serialized Object could not be cast into the given type.")
}
