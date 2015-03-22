package varys.framework.serializer

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import varys.Logging

import scala.reflect.ClassTag

/**
 * Java native serializer
 */
class JavaNativeSerializer extends Serializer with Logging {
  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos : ObjectOutputStream = new ObjectOutputStream(baos)
    oos.writeObject(t)
    oos.flush()
    oos.close()
    ByteBuffer.wrap(baos.toByteArray)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bais: ByteArrayInputStream = new ByteArrayInputStream(bytes.array())
    val ois : ObjectInputStream = new ObjectInputStream(bais)
    val obj = ois.readObject().asInstanceOf[T]
    ois.close()
    obj
  }
}
