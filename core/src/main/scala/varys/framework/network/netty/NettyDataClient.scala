package varys.framework.network.netty

import java.nio.ByteBuffer

import varys.framework.{FileFlowDescription, ObjectFlowDescription, FakeFlowDescription}
import varys.framework.network.DataClient
import varys.framework.network.netty.client.TransportClient

/**
 * Created by hWX221863 on 2015/4/14.
 */
class NettyDataClient(client: TransportClient) extends DataClient {
  override def getFileData(desc: FileFlowDescription): ByteBuffer = {
    null
  }

  override def getObjectData(desc: ObjectFlowDescription): ByteBuffer = ???

  override def getFakeData(desc: FakeFlowDescription): ByteBuffer = ???

  override def updateRate(rate: Double): Unit = ???
}
