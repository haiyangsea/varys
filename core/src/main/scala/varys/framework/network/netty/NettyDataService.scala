package varys.framework.network.netty

import varys.framework.network.{DataClient, DataServer, DataService}

/**
 * Created by Allen on 2015/4/13.
 */
class NettyDataService extends DataService {

  override def getServer: DataServer = ???

  override def getClient(host: String, port: Int): DataClient = ???
}
