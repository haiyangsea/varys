package varys.framework.network

import scala.util.Try

trait DataService {

  def getServer: DataServer

  def getClient(host: String, port: Int): DataClient
}

object DataService {
  import scala.reflect.runtime.{universe => ru}
  private[this] val rm = ru.runtimeMirror(getClass.getClassLoader)

  def getDataService: DataService = {
    val handlerClassName = System.getProperty("varys.data.service")
    try {
      Try(reflectObject(handlerClassName).asInstanceOf[DataService])
        .getOrElse(reflectClass(handlerClassName).asInstanceOf[DataService])
    } catch {
      case e: Throwable =>
        sys.error(s"Failed to build class $handlerClassName as a DataService")
    }
  }

  def reflectObject(fullClassName: String): Any = {
    val module = rm.staticModule(fullClassName)
    rm.reflectModule(module).instance
  }

  def reflectClass(fullClassName: String): Any = {
    val cs = rm.staticClass(fullClassName)
    val cons = cs.typeSignature.declaration(ru.nme.CONSTRUCTOR).asMethod
    val cm = rm.reflectClass(cs)
    cm.reflectConstructor(cons)()
  }
}
