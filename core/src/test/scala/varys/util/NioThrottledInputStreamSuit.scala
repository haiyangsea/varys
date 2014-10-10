package varys.util

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.nio.channels.{SocketChannel, ServerSocketChannel}

import org.scalatest.{BeforeAndAfter, FunSuite}
import varys.framework.serializer.JavaSerializer
import varys.framework.{DataType, FileDescription, GetRequest, NioDataServer}

/**
 * Created by hWX221863 on 2014/10/10.
 */
class NioThrottledInputStreamSuit extends FunSuite with BeforeAndAfter{
  val dataServer = new NioDataServer("10.177.35.224", 0, "NIO-DataServer")
  val serializer = new JavaSerializer
  before {
    dataServer.start()
  }

  test("nio throttled input stream") {
    val size = 31720
    val path = "C:\\Users\\hWX221863\\Desktop\\logs\\varys-root-varys.framework.slave.Slave-ydatasight1.out"
    val flowDesc = new FileDescription("0", path, "0", DataType.ONDISK, 0, size, 1, "10.177.35.224", 0)
    val channel = SocketChannel.open(new InetSocketAddress(dataServer.address, dataServer.port))

    // Don't wait for scheduling for 'SHORT' flows
    val tisRate = if (flowDesc.sizeInBytes > 0) 0.0 else 1024 * 1048576

    val tis = new NioThrottledInputStream(channel, "client", tisRate)
    println("rate is " + tisRate)
    val request = serializer.serialize(GetRequest(flowDesc))
    channel.write(request)
    println("sent request!")
    val baos = new ByteArrayOutputStream()
    val buffer = new Array[Byte](1024)
    var length = tis.read(buffer)
    while(length > 0) {
      println("got response,lenght = " + length)
      baos.write(buffer, 0, length)
      length = tis.read(buffer)
    }
    val data = baos.toByteArray
    baos.close()
    channel.close()
    assert(data.length == size)
  }

  after {
    dataServer.stop()
  }
}
