package varys.framework

import varys.framework.client.{DataCenterAddress, FetchFlowListener, VarysClient}
import varys.framework.serializer.Serializer
import varys.util.NioThrottledInputStream
import varys.{Logging, Utils}

/**
 * Created by hWX221863 on 2014/10/13.
 */
class RemoteDataFetcher(
   channelPool: ChannelPool,
   flows: Seq[FlowDescription],
   listener: FetchFlowListener,
   serializer: Serializer,
   client: VarysClient)
  extends Runnable with Logging {

  val host = flows.head.host
  val port = flows.head.port

  // TODO 添加可控制加载数据量模块，以防OOM
  override def run(): Unit = {
    flows.foreach(flow => {
      val channel = channelPool.getChannel(DataCenterAddress(host, port))
      val rate = if (flow.sizeInBytes > client.SHORT_FLOW_BYTES) 0.0 else client.NIC_BPS
      val stream = new NioThrottledInputStream(channel, client.clientName, rate)
      client.flowToTIS.put(flow.dataId, stream)
      try {
        val request = serializer.serialize(GetRequest(flow))
        channel.write(request)
        val data = new Array[Byte](flow.sizeInBytes.toInt)
        var length = 0
        var offset = 0
        val size = channel.socket().getReceiveBufferSize
        logDebug("Starting to fetch remote data at %s:%d,flow id is %s".format(host, port, flow.id))
        val start = System.currentTimeMillis()
        while(offset < data.length) {
          length = Math.min(data.length, offset + size) - offset
          offset += stream.read(data, offset, length)
        }
        val duration = System.currentTimeMillis() - start
        logDebug("Received remote[%s:%d] data response{size[%s],flow[%s],expected size[%s],duration[%d ms]}"
          .format(host,
            port,
            Utils.bytesToString(length),
            flow.id,
            Utils.bytesToString(flow.sizeInBytes),
            duration))
        listener.complete(flow.id, flow.coflowId, data)
      } catch {
        case e: Throwable => listener.failure(flow.id, flow.coflowId, flow.sizeInBytes, e)
      } finally {
        client.flowToTIS.remove(flow.dataId)
        stream.close()
        channelPool.returnChannel(channel)
      }
    })
  }
}
