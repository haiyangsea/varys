package varys.framework.network.netty

import java.nio.ByteBuffer

import varys.framework.network.netty.server.TransportServer
import varys.framework.{DataIdentifier, ObjectFlowDescription}
import varys.framework.network.DataServer

import scala.collection.mutable.HashMap

/**
 * Created by hWX221863 on 2015/4/14.
 */
class NettyDataServer(server: TransportServer) extends DataServer {
  val flowToObject = new HashMap[DataIdentifier, ByteBuffer]

  override def putObjectData(desc: ObjectFlowDescription, data: ByteBuffer): Unit = {
    flowToObject += desc.dataId -> data
  }

  def getObjectData(desc: ObjectFlowDescription): ByteBuffer = {
    flowToObject.get(desc.dataId).getOrElse(sys.error(s"${desc.dataId} dose not exist!"))
  }

  override def stop(): Unit = server.close()

  override def host: String = server.getHost

  override def port: Int = server.getPort

  override def start(): Unit = server.start()
}
