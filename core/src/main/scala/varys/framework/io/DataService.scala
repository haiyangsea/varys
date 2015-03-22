package varys.framework.io

import java.nio.ByteBuffer

import varys.framework.{FakeFlowDescription, ObjectFlowDescription, FlowDescription, FileFlowDescription}

trait DataService {
  def getFileData(desc: FileFlowDescription): ByteBuffer

  def getObjectData(desc: ObjectFlowDescription): ByteBuffer

  def getFakeData(desc: FakeFlowDescription): ByteBuffer
  
  def getHost: String
  
  def getPort: Int
  
  def start(): Unit
  
  def stop(): Unit
}

object DataServiceFactory {
  def getDataService: DataService = {
    import scala.reflect.runtime.{universe => ru}
    val handlerClassName = System.getProperty("varys.data.service")
    
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val sc = rm.staticClass(handlerClassName)
    val cons = sc.typeSignature.declaration(ru.nme.CONSTRUCTOR).asMethod
    val handler = rm.reflectClass(sc)
    val constructor = handler.reflectConstructor(cons)
    constructor().asInstanceOf[DataService]
  }
}
