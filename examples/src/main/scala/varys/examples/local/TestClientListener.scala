package varys

import varys.framework.client.ClientListener
;

/**
 * Created by Allen on 2015/4/19.
 */
object TestClientListener extends ClientListener {
  // NOT SAFE to use the Client UNTIL this method is called
  override def connected(clientId: String): Unit = {
    println(s"connected, get client id is $clientId")
  }

  override def disconnected(): Unit = {
    println("disconnected!")
  }
}
