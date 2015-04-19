package varys.framework.network

import varys.VarysException

import scala.util.Try

trait DataService {

  def getServer: DataServer

  def getClient(host: String, port: Int, initBitPerSec: Double): DataClient
}

object DataService {
  import scala.reflect.runtime.{universe => ru}
  private[this] val rm = ru.runtimeMirror(getClass.getClassLoader)

  def getDataService: DataService = {
    val handlerClassName = System.getProperty("varys.data.service",
      "varys.framework.network.netty.NettyDataService")
    try {
      Try(reflectObject(handlerClassName).asInstanceOf[DataService])
        .getOrElse(reflectClass(handlerClassName).asInstanceOf[DataService])
    } catch {
      case e: Throwable =>
        throw new VarysException(s"Failed to build class $handlerClassName as a DataService", e)
    }
  }

  private[this] def reflectObject(fullClassName: String): Any = {
    val module = rm.staticModule(fullClassName)
    rm.reflectModule(module).instance
  }

  private[this] def reflectClass(fullClassName: String): Any = {
    val cs = rm.staticClass(fullClassName)
    val cons = cs.typeSignature.declaration(ru.nme.CONSTRUCTOR).asMethod
    val cm = rm.reflectClass(cs)
    cm.reflectConstructor(cons)()
  }
}