package varys.framework.network.netty

import varys.framework.network.netty.client.{RpcResponseCallback, TransportClient}
import varys.framework.network.netty.message.{FileFlowRequest, FlowTransferMessage, OpenFlows}
import varys.framework.network.netty.server.{OneForOneStreamManager, StreamManager, RpcHandler}

/**
 * Created by hWX221863 on 2015/4/14.
 */
class FlowHandler extends RpcHandler {
  val streamManager = new OneForOneStreamManager

  override def receive(
      client: TransportClient,
      message: Array[Byte],
      callback: RpcResponseCallback): Unit = {
     val message = FlowTransferMessage.Decoder.fromByteArray(message)

    message match {
      case openFlows: OpenFlows =>
        openFlows.flowRequests.requests.head match {
          case _: FileFlowRequest =>
            openFlows.flowRequests.requests.map(_.asInstanceOf[FileFlowRequest])
        }

    }
  }

  override def getStreamManager: StreamManager = streamManager
}
