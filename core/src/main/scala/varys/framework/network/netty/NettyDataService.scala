package varys.framework.network.netty

import varys.framework.network.netty.util.{SystemPropertyConfigProvider, TransportConf}
import varys.framework.network.{DataClient, DataServer, DataService}

/**
 * Created by Allen on 2015/4/13.
 */
class NettyDataService extends DataService {

  val context = new TransportContext(
    new TransportConf(new SystemPropertyConfigProvider), new FlowHandler)

  val server = context.createServer()
  val clientFactory = context.createClientFactory()

  override def getServer: DataServer = new NettyDataServer(server)

  override def getClient(host: String, port: Int): DataClient = {
    val client = clientFactory.createClient(host, port)
    new NettyDataClient(client)
  }
}
