package varys.framework.network

import java.nio.ByteBuffer

import varys.framework.network.netty.message.FlowRequestArray

/**
 * Created by Allen on 2015/4/13.
 */
trait DataClient {

  def fetchData(coflowId: String, requests: FlowRequestArray, listener: FlowFetchingListener)

  def updateRate(rate: Double): Unit

  def close(): Unit
}
