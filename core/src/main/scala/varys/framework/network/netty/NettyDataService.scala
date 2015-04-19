package varys.framework.network.netty

import varys.framework.network.netty.util.{SystemPropertyConfigProvider, TransportConf}
import varys.framework.network.{DataClient, DataServer, DataService}

/**
 * Created by Allen on 2015/4/13.
 */
object NettyDataService extends DataService {
  val conf = new TransportConf(new SystemPropertyConfigProvider)
  val context = new TransportContext(conf, new FlowHandler(conf))

  val server = context.createServer()
  val clientFactory = context.createClientFactory()

  override def getServer: DataServer = new NettyDataServer(server)

  override def getClient(host: String, port: Int, initBitPerSec: Double): DataClient = {
    val client = clientFactory.createClient(host, port, initBitPerSec)
    new NettyDataClient(client, conf)
  }
}
