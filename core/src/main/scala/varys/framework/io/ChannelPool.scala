package varys.framework.io

import java.net.InetSocketAddress
import java.nio.channels.SocketChannel

import scala.collection.mutable



/**
 * Created by hWX221863 on 2014/10/15.
 */
class ChannelPool {
  val max = System.getProperty("varys.framework.channel.pool.size", "10").toInt

  private[this] val channels = new mutable.HashMap[DataServerAddress, mutable.Queue[SocketChannel]]

  def getChannel(address: DataServerAddress): SocketChannel = channels.synchronized {
    val queue = channels.get(address)
    if(queue.isDefined && queue.get.size > 0) {
      queue.get.dequeue()
    } else {
      SocketChannel.open(address.netAddress)
    }
  }

  def returnChannel(channel: SocketChannel): Unit = {
    val netAddress = channel.getLocalAddress.asInstanceOf[InetSocketAddress]
    val address = DataServerAddress(netAddress.getHostName, netAddress.getPort)
    channels.synchronized {
      val queue = channels.getOrElseUpdate(address, new mutable.Queue[SocketChannel]())
      if(queue.size < max) {
        queue.enqueue(channel)
      } else {
        channel.close()
      }
    }
  }
  
  def close(): Unit = {
    channels.values.foreach(queue => {
      while(queue.size > 0) {
        queue.dequeue().close()
      }
    })
  }
}

case class DataServerAddress(host: String, port: Int) {
  override def toString = s"$host:$port"

  val netAddress = new InetSocketAddress(host, port)
}
