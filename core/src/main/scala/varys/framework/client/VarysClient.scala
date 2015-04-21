package varys.framework.client

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import akka.actor._
import akka.actor.Terminated
import varys.framework.network.netty.buffer.ManagedBuffer
import varys.framework.network._
import varys.framework.network.netty.message.{FlowRequestArray, FlowRequest, FileFlowRequest}
import varys.framework.serializer.Serializer
import scala.concurrent.duration._
import akka.pattern.ask
import akka.pattern.AskTimeoutException
import akka.remote.{RemotingLifecycleEvent, DisassociatedEvent}

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
  val checkLocality = System.getProperty("varys.client.data.locality", "true").toBoolean

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

  val executors = Executors.newCachedThreadPool()
  // ExecutionContext for Futures
  implicit val futureExecContext = ExecutionContext.fromExecutor(executors)


  var regStartTime = 0L

  val flowToTIS = new ConcurrentHashMap[DataIdentifier, DataClient]()
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

  val serializer: Serializer = Serializer.getSerializer

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
            logDebug("Starting to change flow bit per second.")
            flowToBitPerSec.foreach { kv => {
              // kv (key = FlowId, value = Rate)
              if (flowToTIS.containsKey(kv._1))
                flowToTIS.get(kv._1).updateRate(kv._2)
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
//        flowToTIS.foreach { kv => {
//          // kv (key = dataId, value = TIS)
//          if (kv._1.coflowId == coflowId)
//            kv._2.close()
//          }
//        }

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
    flowToTIS.foreach {
      case (id, client) if id.coflowId == coflowId =>
        logDebug(s"Finished coflow $coflowId, close the client, " +
          s"total sleep time: ${client.sleepTime}.")

        client.close()
      case _ =>
    }

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
    dataServer.putObjectData(desc.dataId, serialObj)
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

  private[this] class FlowListenerWrapper(
      listener: FlowFetchListener,
      flows: Map[String, FlowDescription])
    extends FlowFetchingListener {

    override def onFlowFetchSuccess(coflowId: String, flowId: String, buffer: ManagedBuffer): Unit = {
      val data = buffer.nioByteBuffer()
      masterActor ! FlowProgress(
        flowId,
        coflowId,
        buffer.size(),
        true)
      listener.onFlowFetchSuccess(coflowId, flowId, data)
    }
    override def onFlowFetchFailure(coflowId: String, flowId: String, exception: Throwable): Unit = {
      val length: Long = flows.get(flowId)
        .map(_.sizeInBytes)
        .getOrElse {
        logWarning("flow id dose not exists in requests flows")
        -1L
      }
      masterActor ! FlowProgress(
        flowId,
        coflowId,
        length,
        false)
      listener.onFlowFetchFailure(flowId, coflowId, length, exception)
    }
  }

  def getFlows(flowIds: Seq[String], coflowId: String, listener: FlowFetchListener) {
    waitForRegistration

    val descriptions = AkkaUtils.askActorWithReply[Option[GotFlowDescs]](
      masterActor,
      GetFlows(flowIds.toArray, coflowId, clientId, slaveId))
    val flows = descriptions.map(_.flowDescs)
      .getOrElse(throw new VarysException(s"flows do not exist in coflow $coflowId!"))

    AkkaUtils.tellActor(slaveActor, GetFlows(flowIds.toArray, coflowId, clientId, slaveId, flows))

    val wrappedListener = new FlowListenerWrapper(listener, flows.map(f => (f.id, f)).toMap)
    flows.groupBy(flow => flow.host).foreach {
      case (host, subFlows) =>
        // data in local file system
        if (host == this.slaveHost && checkLocality) {
          logInfo("Starting fetch remote flows data for coflow " +
            coflowId + ", flows : " + subFlows.mkString("[", ",", "]"))
          subFlows.foreach(flow => {
            this.executors.submit(new LocalDataFetcher(flow, wrappedListener))
          })
        } else {
          logInfo("Starting fetch remote flows data for coflow " + coflowId
            + ", flows : " + subFlows.mkString("[", ",", "]"))
          subFlows.groupBy(_.port).map(pair => (pair._1, pair._2.map(_.toRequest))).foreach {
            case (port, remoteFlows) =>
              val tisRate = if (remoteFlows.map(_.size).sum > SHORT_FLOW_BYTES) 0.0 else NIC_BPS
              val client = dataService.getClient(host, port, tisRate)
              // TODO : figure out the right tis
              this.flowToTIS.put(DataIdentifier(remoteFlows.head.flowId, coflowId), client)
              client.fetchData(coflowId, new FlowRequestArray(remoteFlows), wrappedListener)
          }
        }
    }
  }

  implicit class FlowRequestConverter(desc: FlowDescription) {
    def toRequest = desc match {
      case fileFlow: FileFlowDescription =>
        new FileFlowRequest(desc.id, fileFlow.path, fileFlow.offset, fileFlow.length)
      case other => new FlowRequest(desc.id, desc.sizeInBytes)
    }
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
