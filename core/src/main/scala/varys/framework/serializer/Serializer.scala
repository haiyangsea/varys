package varys.framework.serializer

import java.nio.ByteBuffer

import scala.reflect.ClassTag

/**
 * Created by hWX221863 on 2014/9/30.
 */
trait Serializer {
  def serialize[T: ClassTag](t: T): ByteBuffer

  def deserialize[T: ClassTag](bytes: ByteBuffer): T
}

object Serializer {
  def getSerializer: Serializer = {
    val serializerName: String = System.getProperty("varys.framework.serializer",
      "varys.framework.serializer.JavaNativeSerializer")
    Class.forName(serializerName).newInstance().asInstanceOf[Serializer]
  }
}
