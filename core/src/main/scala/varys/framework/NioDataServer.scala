package varys.framework

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels._
import java.util.concurrent.{LinkedBlockingDeque, TimeUnit, ThreadPoolExecutor}

import varys.framework.serializer.Serializer
import varys.{Utils, VarysException, Logging}

import scala.collection.mutable.HashMap

/**
 * Created by hWX221863 on 2014/9/30.
 */
class NioDataServer(
        val address: String,
        initPort: Int,
        val serverName: String,
        flowObjectMap: HashMap[DataIdentifier, Array[Byte]] = null)
  extends Logging {
  val serializer: Serializer = Utils.getSerializer

  val selector: Selector = Selector.open()

  val serverChannel: ServerSocketChannel = ServerSocketChannel.open()
  serverChannel.configureBlocking(false)
  serverChannel.register(selector, SelectionKey.OP_ACCEPT)

  val SELECT_TIMEOUT = System.getProperty("varys.framework.select.timeout", "100").toInt
  val threadMin = System.getProperty("varys.framework.server.threads.min", "1").toInt
  val threadMax = System.getProperty("varys.framework.server.threads.min", "8").toInt
  val aliveTime = System.getProperty("varys.framework.server.alive", "60").toInt

  private val requestExecutor = new ThreadPoolExecutor(
    threadMin,
    threadMax,
    aliveTime, TimeUnit.SECONDS,
    new LinkedBlockingDeque[Runnable](),
    Utils.daemonThreadFactory)

  val selectorThread: Thread = new Thread("selector-thread") {
    override def run(): Unit ={
      NioDataServer.this.run()
    }
  }
  selectorThread.setDaemon(true)

  def start(): Unit = {
    val maxRetries = System.getProperty("varys.port.maxRetries", "5").toInt
    Range(0, maxRetries).foreach(offset => {
      try {
        serverChannel.bind(new InetSocketAddress(address, initPort + offset))
        logInfo("nio data server start at %s:%s"
          .format(serverChannel.socket().getInetAddress.getHostAddress, port))
        selectorThread.start()
        return
      } catch {
        case e: Exception => {
          if(offset == maxRetries - 1) {
            throw new VarysException(s"data server start failed after $maxRetries", e)
          }
          logWarning(s"data server could not bind on port $initPort. " +
            s"Attempting port ${initPort + offset}.")
        }
      }
    })
  }

  def stop(): Unit = {
    selectorThread.interrupt()
    selectorThread.join()
    selector.close()
    requestExecutor.shutdown()
  }

  def port: Int = serverChannel.socket.getLocalPort

  def run(): Unit ={
    try {
      while(!selectorThread.isInterrupted) {
        val length = selector.select(SELECT_TIMEOUT)
        if(length > 0) {
          val keys = selector.selectedKeys().iterator()
          while(keys.hasNext) {
            val key = keys.next()
            keys.remove()
            if(key.isValid) {
              if(key.isAcceptable) {
                val server = key.channel().asInstanceOf[ServerSocketChannel]
                val channel = server.accept()
                channel.configureBlocking(false)
                channel.register(selector, SelectionKey.OP_READ)
                logDebug("got client[%s] connection".format(channel.getRemoteAddress))
              } else if(key.isReadable) {
                key.interestOps(SelectionKey.OP_CONNECT)
                requestExecutor.execute(new RequestHandler(key, doRead))
              } else if(key.isWritable) {
                key.interestOps(SelectionKey.OP_CONNECT)
                requestExecutor.execute(new RequestHandler(key, doWrite))
              }
            }
          }
        }
      }
    } catch {
      case e: Exception => logError("Error in select loop", e)
    }
  }

  def doRead(key: SelectionKey): Unit = {
    val channel = key.channel().asInstanceOf[SocketChannel]
    logDebug("starting to deal with data request from " + channel.getRemoteAddress.toString)
    val data = Utils.readFromChannel(channel)
    val request: GetRequest = serializer.deserialize[GetRequest](ByteBuffer.wrap(data))
    channel.register(selector, SelectionKey.OP_WRITE, request)
  }

  def doWrite(key: SelectionKey): Unit ={
    val channel = key.channel().asInstanceOf[SocketChannel]
    try {
      val request = key.attachment().asInstanceOf[GetRequest]
      logDebug("starting send data for request[%s]".format(request.flowDesc.toString))
      val dataType = request.flowDesc.dataType
      // specially for fake data type
      if(dataType == DataType.FAKE) {
        handleFakeRequest(request, channel)
      } else {
        val message: Array[Byte] = if (dataType == DataType.ONDISK) {
          val desc = request.flowDesc.asInstanceOf[FileDescription]
          getFileData(desc)
        } else if (dataType == DataType.INMEMORY) {
          getObjectData(request.flowDesc)
        } else {
          Array[Byte]()
        }
        channel.write(ByteBuffer.wrap(message))
        logDebug("send data successfully,size is %s,flow is %s".format(Utils.bytesToString(message.length),
          request.flowDesc.dataId.dataId))
      }
    } catch {
      case e: Exception =>
        logError("Failed to handle remote request", e)
    } finally {
      channel.close()
    }
  }

  def getFileData(desc: FileDescription): Array[Byte] = {
    logInfo("receive get file[%s] request".format(desc.pathToFile))
    val data = Utils.readFileUseNIO(desc)
    data
  }

  def getObjectData(desc: FlowDescription): Array[Byte] = {
    if (flowObjectMap != null && flowObjectMap.contains(desc.dataId))
      flowObjectMap(desc.dataId)
    else {
      logWarning("Requested object does not exist!" + flowObjectMap)
      Array[Byte]()
    }
  }

  def handleFakeRequest(request: GetRequest, channel: SocketChannel): Unit ={
    val buf = ByteBuffer.allocate(65536)
    var bytesSent = 0L
    while (bytesSent < request.flowDesc.sizeInBytes) {
      buf.clear()
      val bytesToSend =
        math.min(request.flowDesc.sizeInBytes - bytesSent, buf.limit())
      buf.limit(bytesToSend.toInt)
      channel.write(buf)
      bytesSent += bytesToSend
    }
  }

  private[this] class RequestHandler(key: SelectionKey, doWorker: SelectionKey => Unit) extends Runnable {
    override def run(): Unit = {
      if(doWorker != null && key != null) {
        doWorker.apply(key)
      }
    }
  }
}
