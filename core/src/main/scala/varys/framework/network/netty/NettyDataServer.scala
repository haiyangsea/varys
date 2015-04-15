package varys.framework.network.netty

import java.nio.ByteBuffer

import varys.Utils
import varys.framework.network.netty.server.TransportServer
import varys.framework.{DataIdentifier, ObjectFlowDescription}
import varys.framework.network.DataServer

import scala.collection.mutable.HashMap

/**
 * Created by hWX221863 on 2015/4/14.
 */
class NettyDataServer(server: TransportServer) extends DataServer {
  val flowToObject = new HashMap[DataIdentifier, ByteBuffer]

  override def putObjectData(id: DataIdentifier, data: ByteBuffer): Unit = {
    flowToObject += id -> data
  }

  def getObjectData(id: DataIdentifier): ByteBuffer = {
    flowToObject.get(id).getOrElse(sys.error(s"$id dose not exist!"))
  }

  override def stop(): Unit = server.close()

  override def host: String = Utils.localIpAddress

  override def port: Int = server.getPort

  override def start(): Unit = server.start()
}
