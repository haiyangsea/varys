package varys.framework

import java.net.InetSocketAddress
import java.nio.channels.SocketChannel

import varys.framework.io.DataCenterAddress

import scala.collection.mutable



/**
 * Created by hWX221863 on 2014/10/15.
 */
class ChannelPool {
  val max = System.getProperty("varys.framework.channel.pool.size", "10").toInt

  private[this] val channels = new mutable.HashMap[DataCenterAddress, mutable.Queue[SocketChannel]]

  def getChannel(address: DataCenterAddress): SocketChannel = channels.synchronized {
    val queue = channels.get(address)
    if(queue.isDefined && queue.get.size > 0) {
      queue.get.dequeue()
    } else {
      SocketChannel.open(address.socketAddress)
    }
  }

  def returnChannel(channel: SocketChannel): Unit = {
    val netAddress = channel.getLocalAddress.asInstanceOf[InetSocketAddress]
    val address = DataCenterAddress(netAddress.getHostName, netAddress.getPort)
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

