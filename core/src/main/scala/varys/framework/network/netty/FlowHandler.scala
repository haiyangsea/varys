package varys.framework.network.netty

import varys.Logging
import varys.framework.network.netty.client.{RpcResponseCallback, TransportClient}
import varys.framework.network.netty.message.{UploadFlows, FlowTransferMessage, OpenFlows}
import varys.framework.network.netty.protocol.StreamHandle
import varys.framework.network.netty.server.{OneForOneStreamManager, StreamManager, RpcHandler}
import varys.framework.network.netty.util.TransportConf

/**
 * Created by hWX221863 on 2015/4/14.
 */
class FlowHandler(conf: TransportConf) extends RpcHandler with Logging {
  val streamManager = new OneForOneStreamManager

  override def receive(
      client: TransportClient,
      msg: Array[Byte],
      callback: RpcResponseCallback): Unit = {
     val message = FlowTransferMessage.Decoder.fromByteArray(msg)

    message match {
      case openFlows: OpenFlows =>
        val buffers = openFlows.flowRequests.toManagedBuffer(conf)
        val streamId = streamManager.registerStream(buffers)
        callback.onSuccess(new StreamHandle(streamId, openFlows.flowRequests.length).toByteArray)
      case uploadFlows: UploadFlows =>
        logInfo("Get upload flows message in FlowHandler")
    }
  }

  override def getStreamManager: StreamManager = streamManager
}
