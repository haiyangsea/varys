package varys.framework.network.netty

import varys.Logging
import varys.framework.network.netty.message.FlowRequestArray
import varys.framework.network.netty.util.TransportConf
import varys.framework.network.{FlowFetchingListener, DataClient}
import varys.framework.network.netty.client.TransportClient

/**
 * Created by hWX221863 on 2015/4/14.
 */
class NettyDataClient(
    client: TransportClient,
    conf: TransportConf)
  extends DataClient with Logging {

  override def fetchData(
    coflowId: String,
    requests: FlowRequestArray,
    listener: FlowFetchingListener): Unit = {
    try {
      val blockFetchStarter = new RetryingFlowFetcher.FlowFetchStarter {
        override def createAndStart(requests: FlowRequestArray,
                                    listener: FlowFetchingListener) {
          new OneForOneFlowFetcher(client, coflowId, requests, listener).start()
        }
      }

      val maxRetries = conf.maxIORetries()
      logDebug("Starting to fetch flow with max retries " + maxRetries)
      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingFlowFetcher(conf, blockFetchStarter, coflowId, requests, listener).start()
      } else {
        blockFetchStarter.createAndStart(requests, listener)
      }
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        requests.requests.foreach(r => listener.onFlowFetchFailure(coflowId, r.flowId, e))
    }
  }

  override def updateRate(rate: Double): Unit = client.throttle.updateRate(rate)

  override def sleepTime = client.throttle.getTotalSleepTime()

  override def close(): Unit = {
    client.close()
  }
}
