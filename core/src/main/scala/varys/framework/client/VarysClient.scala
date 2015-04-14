package varys.framework.client

import java.nio.channels.SocketChannel

import akka.actor._
import akka.actor.Terminated
import varys.framework.network.DataService
import varys.framework.serializer.Serializer
import scala.concurrent.duration._
import akka.pattern.ask
import akka.pattern.AskTimeoutException
import akka.remote.{RemotingLifecycleEvent, DisassociatedEvent}

import java.net._
import java.util.concurrent.{Executors, ConcurrentHashMap}

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import varys.{Logging, Utils, VarysException}
import varys.framework._
import varys.framework.master.Master
import varys.framework.slave.Slave
import varys.util._
import scala.concurrent.{ExecutionContext, Await}

// TODO 整合VarysClient和Slave中的数据存储,优化Flow数据获取
class VarysClient(
    val clientName: String,
    masterUrl: String,
    listener: ClientListener = null)
  extends Logging {

  val INTERNAL_ASK_TIMEOUT_MS: Int = 
    System.getProperty("varys.client.internalAskTimeoutMillis", "5000").toInt
  val RATE_UPDATE_FREQ = System.getProperty("varys.client.rateUpdateIntervalMillis", "100").toLong
  val SHORT_FLOW_BYTES = System.getProperty("varys.client.shortFlowMB", "0").toLong * 1048576
  val NIC_BPS = 1024 * 1048576

  val MAX_FLOWS_REQUEST = System.getProperty("vayrs.client.request.flows.max", "1").toInt

  var actorSystem: ActorSystem = null
  
  var masterActor: ActorRef = null
  val clientRegisterLock = new Object
  
  var slaveId: String = null
  var slaveUrl: String = null
  var slaveActor: ActorRef = null
  
  var clientId: String = null
  var clientActor: ActorRef = null

  val dataService  = DataService.getDataService

  // ExecutionContext for Futures
  implicit val futureExecContext = ExecutionContext.fromExecutor(Utils.newDaemonCachedThreadPool())
  val executors = Executors.newCachedThreadPool()

  var regStartTime = 0L

  val flowToTIS = new ConcurrentHashMap[DataIdentifier, NioThrottledInputStream]()
  // TODO: Currently using flowToBitPerSec inside synchronized blocks. Might consider replacing with
  // an appropriate data structure; e.g., Collections.synchronizedMap.
  val flowToBitPerSec = new ConcurrentHashMap[DataIdentifier, Double]()
  val flowToObject = new HashMap[DataIdentifier, Array[Byte]]

  val serverThreadName = "ServerThread for Client@" + Utils.localHostName()
  var dataServer = dataService.getServer
  dataServer.start()

  var clientHost = dataServer.host
  var slaveHost: String = null
  var clientPort = dataServer.port

  val serializer: Serializer = Utils.getSerializer

  class ClientActor extends Actor with Logging {
    var masterAddress: Address = null

    // To avoid calling listener.disconnected() multiple times
    var alreadyDisconnected = false  

    override def preStart() {
      logInfo("Connecting to master " + masterUrl)
      regStartTime = now
      try {
        masterActor = AkkaUtils.getActorRef(Master.toAkkaUrl(masterUrl), context)
        masterAddress = masterActor.path.address
        masterActor ! RegisterClient(clientName, clientHost, clientPort)
        context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
      } catch {
        case e: Exception =>
          logError("Failed to connect to master", e)
          markDisconnected()
          context.stop(self)
      }
    }

    @throws(classOf[VarysException])
    def masterDisconnected() {
      // TODO: It would be nice to try to reconnect to the master, but just shut down for now.
      // (Note that if reconnecting we would also need to assign IDs differently.)
      val connToMasterFailedMsg = "Connection to master failed; stopping client"
      logWarning(connToMasterFailedMsg)
      markDisconnected()
      context.stop(self)
      throw new VarysException(connToMasterFailedMsg)
    }

    override def receive = {
      
      case RegisteredClient(clientId_, slaveId_, slaveUrl_) =>
        logDebug(s"got master register reply[client id = $clientId_,slave id = $slaveId_,slave url = $slaveUrl_]")

        clientId = clientId_
        slaveId = slaveId_
        slaveUrl = slaveUrl_
        slaveActor = AkkaUtils.getActorRef(Slave.toAkkaUrl(slaveUrl), context)
        slaveHost = Slave.getSlaveHost(slaveUrl)

        if (listener != null) {
          listener.connected(clientId)
        }
        clientRegisterLock.synchronized { clientRegisterLock.notifyAll() }  // Ready to go!
        logInfo("Registered to master in " +  (now - regStartTime) + 
          " milliseconds. Local slave url = " + slaveUrl)
        
        // Thread to periodically uodate the rates of all existing ThrottledInputStreams
        context.system.scheduler.schedule(0 millis, RATE_UPDATE_FREQ millis) {
          flowToBitPerSec.synchronized {
            flowToBitPerSec.foreach { kv => {
              // kv (key = FlowId, value = Rate)
              if (flowToTIS.containsKey(kv._1)) 
                flowToTIS.get(kv._1).setNewRate(kv._2)
              }
            }
          }
        }
      case RegisterClientFailed(message) =>
        throw new VarysException("register client failed,cause " + message)

      case Terminated(actor_) if actor_ == masterActor =>
        masterDisconnected()

      case e: DisassociatedEvent if e.remoteAddress == masterAddress =>
        masterDisconnected()

//      case RemoteClientShutdown(_, address) if address == masterAddress =>
//        masterDisconnected()

      case StopClient =>
        markDisconnected()
        sender ! true
        context.stop(self)
        
      case UpdatedRates(newRates) => 
        logInfo("Received updated shares")
        flowToBitPerSec.synchronized {
          for ((dataId, newBitPerSec) <- newRates) {
            logDebug(dataId + " ==> " + newBitPerSec + " bps")
            flowToBitPerSec.put(dataId, newBitPerSec)
          }
        }

      case RejectedCoflow(coflowId, rejectMessage) =>
        logDebug("Coflow " + coflowId + " has been rejected! " + rejectMessage)

        // Let the client know
        if (listener != null) {
          listener.coflowRejected(coflowId, rejectMessage)
        }

        // Close ongoing streams, if any. This will raise exceptions in getOne() 
        // and go back to the application.
        // TODO: Find a more elegant solution.
        flowToTIS.foreach { kv => {
          // kv (key = dataId, value = TIS)
          if (kv._1.coflowId == coflowId)
            kv._2.close()
          }
        }

        // Free local resources
        freeLocalResources(coflowId)
    }

    /**
     * Notify the listener that we disconnected, if we hadn't already done so before.
     */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        if (listener != null) {
          listener.disconnected()
        }
        alreadyDisconnected = true
      }
    }
    
  }

  private def now() = System.currentTimeMillis

  def start() {
    // Just launch an actor; it will call back into the listener.
    val (actorSystem_, _) = AkkaUtils.createActorSystem("varysClient", Utils.localIpAddress, 0)
    actorSystem = actorSystem_
    clientActor = actorSystem.actorOf(Props(new ClientActor))
  }

  def stop() {
    if (clientActor != null) {
      try {
        val timeout = INTERNAL_ASK_TIMEOUT_MS.millis
        val future = clientActor.ask(StopClient)(timeout)
        Await.result(future, timeout)
      } catch {
        case e: AskTimeoutException =>  // Ignore it, maybe master went away
      }
      clientActor = null
    }
    dataServer.stop()
    executors.shutdown()
  }
  
  def awaitTermination() { 
    actorSystem.awaitTermination() 
  }
  
  // Wait until the client has been registered
  private def waitForRegistration = {
    while (clientId == null) {
      clientRegisterLock.synchronized { 
        clientRegisterLock.wait()
        clientRegisterLock.notifyAll()
      }
    }
  }
  
  def registerCoflow(coflowDesc: CoflowDescription): String = {
    waitForRegistration
    
    // Register with the master
    val RegisteredCoflow(coflowId) = AkkaUtils.askActorWithReply[RegisteredCoflow](masterActor, 
      RegisterCoflow(clientId, coflowDesc))
      
    // Let the local slave know
    AkkaUtils.tellActor(slaveActor, RegisteredCoflow(coflowId))
    
    coflowId
  }
  
  def unregisterCoflow(coflowId: String) {
    waitForRegistration
    
    // Let the master know
    AkkaUtils.tellActor(masterActor, UnregisterCoflow(coflowId))
    
    // Update local slave
    AkkaUtils.tellActor(slaveActor, UnregisterCoflow(coflowId))
    
    // Free local resources
    freeLocalResources(coflowId)
  }

  private def freeLocalResources(coflowId: String) {
    flowToTIS.retain((dataId, _) => dataId.coflowId != coflowId)
    flowToBitPerSec.synchronized { 
      flowToBitPerSec.retain((dataId, _) => dataId.coflowId != coflowId) 
    }
    flowToObject.retain((dataId, _) => dataId.coflowId != coflowId)
  }

  /**
   * Makes data available for retrieval, and notifies local slave, which will register it with the 
   * master.
   * Non-blocking call.
   */
  private def handlePut(flowDesc: FlowDescription, serialObj: Array[Byte] = null) {
    waitForRegistration
    
    val st = now
    
    // Notify the slave, which will notify the master
    AkkaUtils.tellActor(slaveActor, AddFlow(flowDesc))
    
    logInfo("Registered " + flowDesc + " in " + (now - st) + " milliseconds")
  }

  /**
   * Makes multiple pieces of data available for retrieval, and notifies local slave, which will 
   * register it with the master. 
   * Non-blocking call.
   * FIXME: Handles only DataType.FAKE right now.
   */
  private def handlePutMultiple(
      flowDescs: Array[FlowDescription],
      coflowId: String, 
      dataType: DataType.DataType) {
    
    if (dataType != DataType.FAKE) {
      val tmpM = "handlePutMultiple currently supports only DataType.FAKE"
      logWarning(tmpM)
      throw new VarysException(tmpM)
    }
    
    waitForRegistration
  
    val st = now
  
    // Notify the slave, which will notify the master
    AkkaUtils.tellActor(slaveActor, AddFlows(flowDescs, coflowId, dataType))
  
    logInfo("Registered Array[FlowDescription] in " + (now - st) + " milliseconds")
  }

  /**
   * Puts any data structure
   */
  def putObject[T: Manifest](
      objId: String, 
      obj: T, 
      coflowId: String, 
      size: Long, 
      numReceivers: Int) {
    
    // TODO: Figure out class name
    val className = Utils.getClassName(obj)
    val desc = 
      new ObjectFlowDescription(
        objId, 
        className, 
        coflowId, 
        DataType.INMEMORY, 
        size, 
        numReceivers, 
        clientHost, 
        clientPort)

    val serialObj = serializer.serialize(obj)
//    handlePut(desc, serialObj)
    dataServer.putObjectData(desc, serialObj)
  }
  
  /**
   * Puts a complete local file
   */
  def putFile(fileId: String, pathToFile: String, coflowId: String, size: Long, numReceivers: Int) {
    putFile(fileId, pathToFile, coflowId, 0, size, numReceivers)
  }

  /**
   * Puts a range of local file
   */
  def putFile(
      fileId: String, 
      pathToFile: String, 
      coflowId: String, 
      offset: Long, 
      size: Long, 
      numReceivers: Int) {
    
    val desc = 
      new FileFlowDescription(
        fileId, 
        pathToFile, 
        coflowId, 
        DataType.ONDISK, 
        offset, 
        size, 
        numReceivers, 
        clientHost, 
        clientPort)

    handlePut(desc)
  }
  
  /**
   * Emulates the process without having to actually put anything
   */
  def putFake(blockId: String, coflowId: String, size: Long, numReceivers: Int) {
    val desc = 
      new FlowDescription(
        blockId, 
        coflowId, 
        DataType.FAKE, 
        size, 
        numReceivers, 
        clientHost,
        clientPort)

    handlePut(desc)
  }
  
  /**
   * Puts multiple blocks at the same time of same size and same number of receivers
   * blocks => (flowId, blockSize, numReceivers)
   */ 
  def putFakeMultiple(blocks: Array[(String, Long, Int)], coflowId: String) {
    val descs = 
      blocks.map(blk => 
        new FlowDescription(
          blk._1, 
          coflowId, 
          DataType.FAKE, 
          blk._2, 
          blk._3,
          clientHost,
          clientPort))

    handlePutMultiple(descs, coflowId, DataType.FAKE)
  }

  /**
   * Performs exactly one get operation
   */
  @throws(classOf[VarysException])
  private def getOne(flowDesc: FlowDescription): (FlowDescription, Array[Byte]) = {
    // file in local file system,read it directly using nio instead of through net
    if(flowDesc.dataType == DataType.ONDISK && flowDesc.host == this.slaveHost) {
      val desc = flowDesc.asInstanceOf[FileFlowDescription]
      logInfo("Data[%s] is in local file system,just read it directly".format(desc.pathToFile))
      val data = Utils.readFileUseNIO(desc)
      return (flowDesc, data)
    }

    var st = now
    val channel = SocketChannel.open(new InetSocketAddress(flowDesc.host, flowDesc.port))

    // Don't wait for scheduling for 'SHORT' flows
    val tisRate = if (flowDesc.sizeInBytes > SHORT_FLOW_BYTES) 0.0 else NIC_BPS

    val tis = new NioThrottledInputStream(channel, clientName, tisRate)
    flowToTIS.put(flowDesc.dataId, tis)

    val buffer = serializer.serialize(GetRequest(flowDesc))
    logDebug("Serialized get request object,size is " + buffer.remaining())
    channel.write(buffer)

    var retVal: Array[Byte] = null

    st = now
    // Specially handle DataType.FAKE
    if (flowDesc.dataType == DataType.FAKE) {
      val buf = new Array[Byte](65536)
      var bytesReceived = 0L
      while (bytesReceived < flowDesc.sizeInBytes) {
        val n = tis.read(buf)
        // logInfo("Received " + n + " bytes of " + flowDesc.sizeInBytes)
        if (n == -1) {
          logError("EOF reached after " + bytesReceived + " bytes")
          throw new VarysException("Too few bytes received")
        } else {
          bytesReceived += n
        }
      }
    } else {
      retVal = new Array[Byte](flowDesc.sizeInBytes.toInt)
      val receiveBufferSize = channel.socket().getReceiveBufferSize
      var length = 0
      var offset = 0
      logDebug("Starting to fetch remote data at %s:%d,receive buffer size %s".format(
        flowDesc.host, flowDesc.port, Utils.bytesToString(receiveBufferSize)))

      val start = System.currentTimeMillis()
      while(offset < retVal.length) {
        length = Math.min(retVal.length, offset + receiveBufferSize) - offset
        offset += tis.read(retVal, offset, length)
      }
      val duration = System.currentTimeMillis() - start
      logInfo("Received remote[%s:%d] data response{size[%s],flow[%s],expected size[%s],duration[%d ms]}"
        .format(flowDesc.host,
          flowDesc.port,
          Utils.bytesToString(length),
          flowDesc.dataId.dataId,
          Utils.bytesToString(flowDesc.sizeInBytes),
          duration))
      if(offset != flowDesc.sizeInBytes) {
        throw new VarysException("Data size dose not match,got data size %d,but expected %d"
          .format(retVal.length, flowDesc.sizeInBytes))
      }
    }
    logTrace("Received " + flowDesc.sizeInBytes + " bytes for " + flowDesc + " in " + (now - st) +
      " milliseconds")

    // Close everything
    flowToTIS.remove(flowDesc.dataId)
    tis.close
    channel.close

    (flowDesc, retVal)
  }
  
  /**
   * Notifies the master and the slave. But everything is done in the client
   * Blocking call.
   */
  @throws(classOf[VarysException])
  private def handleGet(
      blockId: String, 
      dataType: DataType.DataType, 
      coflowId: String): Array[Byte] = {
    
    waitForRegistration
    
    var st = now
    
    // Notify master and retrieve the FlowDescription in response
    var flowDesc: FlowDescription = null

    val gotFlowDesc = AkkaUtils.askActorWithReply[Option[GotFlowDesc]](masterActor, 
      GetFlow(blockId, coflowId, clientId, slaveId))
    gotFlowDesc match {
      case Some(GotFlowDesc(x)) => flowDesc = x
      case None => { 
        val tmpM = "Failed to receive FlowDescription for " + blockId + " of coflow " + coflowId
        logWarning(tmpM)
        // TODO: Define proper VarysExceptions
        throw new VarysException(tmpM)
      }
    }
    logInfo("Received " + flowDesc + " for " + blockId + " of coflow " + coflowId + " in " + 
      (now - st) + " milliseconds")
    
    // Notify local slave
    AkkaUtils.tellActor(slaveActor, GetFlow(blockId, coflowId, clientId, slaveId, flowDesc))
    
    // Get it!
    val (origFlowDesc, retVal) = getOne(flowDesc)
    // Notify flow completion
    masterActor ! FlowProgress(
      origFlowDesc.id, 
      origFlowDesc.coflowId, 
      origFlowDesc.sizeInBytes, 
      true)

    retVal
  }
  
  /**
   * Notifies the master and the slave. But everything is done in the client
   * Blocking call.
   * FIXME: Handles only DataType.FAKE right now.
   */
  @throws(classOf[VarysException])
  private def handleGetMultiple(
      blockIds: Array[String], 
      dataType: DataType.DataType, 
      coflowId: String) {
    
    if (dataType != DataType.FAKE) {
      val tmpM = "handleGetMultiple currently supports only DataType.FAKE"
      logWarning(tmpM)
      throw new VarysException(tmpM)
    }

    waitForRegistration
    
    var st = now
    
    // Notify master and retrieve the FlowDescription in response
    var flowDescs: Array[FlowDescription] = null

    val gotFlowDescs = AkkaUtils.askActorWithReply[Option[GotFlowDescs]](
      masterActor, 
      GetFlows(blockIds, coflowId, clientId, slaveId))

    gotFlowDescs match {
      case Some(GotFlowDescs(x)) => flowDescs = x
      case None => { 
        val tmpM = "Failed to receive FlowDescriptions for " + blockIds.size + " flows of coflow " + 
          coflowId
        logWarning(tmpM)
        // TODO: Define proper VarysExceptions
        throw new VarysException(tmpM)
      }
    }
    logInfo("Received " + flowDescs.size + " flowDescs " + " of coflow " + coflowId + " in " + 
      (now - st) + " milliseconds")
    
    // Notify local slave
    AkkaUtils.tellActor(slaveActor, GetFlows(blockIds, coflowId, clientId, slaveId, flowDescs))
    
    // Get 'em!
    val recvLock = new Object
    var recvFinished = 0

    for (flowDesc <- flowDescs) {
      new Thread("Receive thread for " + flowDesc) {
        override def run() {
          val (origFlowDesc, retVal) = getOne(flowDesc)
          // Notify flow completion
          masterActor ! FlowProgress(
            origFlowDesc.id, 
            origFlowDesc.coflowId, 
            origFlowDesc.sizeInBytes, 
            true)
          
          recvLock.synchronized {
            recvFinished += 1
            recvLock.notifyAll()
          }
        }
      }.start()
    }

    recvLock.synchronized {
      while (recvFinished < flowDescs.size) {
        recvLock.wait()
      }
    }

    // val futureList = Future.traverse(flowDescs.toList)(fd => Future(getOne(fd)))
    // futureList.onComplete {
    //   case Right(arrayOfFlowDesc_Res) => {
    //     arrayOfFlowDesc_Res.foreach { x =>
    //       val (origFlowDesc, res) = x
    //       masterActor ! FlowProgress(
    //         origFlowDesc.id, 
    //         origFlowDesc.coflowId, 
    //         origFlowDesc.sizeInBytes, 
    //         true)
    //     }
    //   }
    //   case Left(vex) => throw vex
    // }
  }

  /**
   * Retrieves data from any of the feasible locations. 
   */
  @throws(classOf[VarysException])
  def getObject[T](objectId: String, coflowId: String): T = {
    val resp = handleGet(objectId, DataType.INMEMORY, coflowId)
    Utils.deserialize[T](resp)
  }
  
  /**
   * Gets a file
   */
  @throws(classOf[VarysException])
  def getFile(fileId: String, coflowId: String): Array[Byte] = {
    handleGet(fileId, DataType.ONDISK, coflowId)
  }

  private[this] class FlowListenerWrapper(listener: FetchFlowListener) extends FetchFlowListener {
    override def complete(flowId: String, coflowId: String, data: Array[Byte]): Unit = {
      masterActor ! FlowProgress(
        flowId,
        coflowId,
        data.length,
        true)
      listener.complete(flowId, coflowId, data)
    }
    override def failure(flowId: String, coflowId: String, length: Long, e: Throwable): Unit = {
      masterActor ! FlowProgress(
        flowId,
        coflowId,
        length,
        false)
      listener.failure(flowId, coflowId, length: Long, e)
    }
  }

  def getFlows(flowIds: Seq[String], coflowId: String, listener: FetchFlowListener) {
    val descriptions = AkkaUtils.askActorWithReply[Option[GotFlowDescs]](
      masterActor,
      GetFlows(flowIds.toArray, coflowId, clientId, slaveId))
    if(!descriptions.isDefined) {
      throw new VarysException(s"flows do not exist in coflow $coflowId!")
    }
    val flows = descriptions.get.flowDescs
    AkkaUtils.tellActor(slaveActor, GetFlows(flowIds.toArray, coflowId, clientId, slaveId, flows))
    logInfo("Starting fetch remote flows data for coflow " + coflowId)
    flows.groupBy(flow => flow.host).foreach(pair => {
      val dataListener = new FlowListenerWrapper(listener)
      val (host, flows) = pair
      if(host == this.slaveHost) {
        flows.foreach(flow => {
          this.executors.submit(new LocalDataFetcher(flow, dataListener))
        })
      } else {
        var left = flows
        do {
          val parts = left.splitAt(MAX_FLOWS_REQUEST)
          left = parts._2
          val fetcher = new RemoteDataFetcher(channelPool, parts._1.toSeq, dataListener, this.serializer, this)
          this.executors.submit(fetcher)
        } while(left.length > 0)
      }
    })
  }


  /**
   * Paired get() for putFake. Doesn't return anything, but emulates the retrieval process.
   */
  @throws(classOf[VarysException])
  def getFake(blockId: String, coflowId: String) {
    handleGet(blockId, DataType.FAKE, coflowId)
  }
  
  /**
   * Paired get() for putFakeMultiple. Doesn't return anything, but emulates the retrieval process.
   */
  @throws(classOf[VarysException])
  def getFakeMultiple(blockIds: Array[String], coflowId: String) {
    handleGetMultiple(blockIds, DataType.FAKE, coflowId)
  }

  def deleteFlow(flowId: String, coflowId: String) {
    // TODO: Do something!
    // AkkaUtils.tellActor(slaveActor, DeleteFlow(flowId, coflowId))
  }

  /**
   * Receive 'howMany' machines with the lowest incoming usage
   */
  def getBestRxMachines(howMany: Int, adjustBytes: Long): Array[String] = {
    val BestRxMachines(bestRxMachines) = AkkaUtils.askActorWithReply[BestRxMachines](
      masterActor,  
      RequestBestRxMachines(howMany, adjustBytes))
    bestRxMachines
  }

  /**
   * Receive the machine with the lowest incoming usage
   */
  def getBestRxMachine(adjustBytes: Long): String = {
    val BestRxMachines(bestRxMachines) = AkkaUtils.askActorWithReply[BestRxMachines](
      masterActor,  
      RequestBestRxMachines(1, adjustBytes))
    bestRxMachines(0)
  }

  /**
   * Receive 'howMany' machines with the lowest outgoing usage
   */
  def getTxMachines(howMany: Int, adjustBytes: Long): Array[String] = {
    val BestTxMachines(bestTxMachines) = AkkaUtils.askActorWithReply[BestTxMachines](
      masterActor,  
      RequestBestTxMachines(howMany, adjustBytes))
    bestTxMachines
  }
  
  /**
   * Receive the machine with the lowest outgoing usage
   */
  def getBestTxMachine(adjustBytes: Long): String = {
    val BestTxMachines(bestTxMachines) = AkkaUtils.askActorWithReply[BestTxMachines](
      masterActor,  
      RequestBestTxMachines(1, adjustBytes))
    bestTxMachines(0)
  }
}
