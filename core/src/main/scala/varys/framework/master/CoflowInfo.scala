package varys.framework.master

import akka.actor.ActorRef

import java.util.Date
import java.util.concurrent.atomic._
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

import varys.framework.{FlowDescription, CoflowDescription}
import varys.Logging
import scala.collection.mutable

private[varys] class CoflowInfo(
    val startTime: Long,
    val id: String,
    val desc: CoflowDescription,
    val parentClient: ClientInfo,
    val submitDate: Date,
    val actor: ActorRef) extends Logging {
  
  private var _prevState = CoflowState.WAITING
  def prevState = _prevState
  
  private var _curState = CoflowState.WAITING
  def curState = _curState
  
  def changeState(nextState: CoflowState.Value) {
    _prevState = _curState
    _curState = nextState
  }

  var origAlpha = 0.0
  
  var bytesLeft_ = new AtomicLong(0L)
  def bytesLeft: Long = bytesLeft_.get()
  
  var readyTime = -1L
  var endTime = -1L

  // Data structure to keep track of total allocation of this coflow over time. Initialized with 0.0
  var allocationOverTime = new ArrayBuffer[(Long, Double)]()
  allocationOverTime += ((startTime, 0.0))
  
  private val idToFlow = new ConcurrentHashMap[String, FlowInfo]()

  private var _retryCount = 0
  def retryCount = _retryCount

  private val numRegisteredFlows = new AtomicInteger(0)
  private val numCompletedFlows = new AtomicInteger(0)

  def numFlowsToRegister = desc.maxFlows - numRegisteredFlows.get
  def numFlowsToComplete = desc.maxFlows - numCompletedFlows.get
  
  def getFlows() = idToFlow.values.asScala.filter(_.isLive)

  def flows = idToFlow.values.asScala

  def size: Long = flows.map(_.getFlowSize()).sum

  // all host throughput, host name, input size, output size
  def hostThroughput: Iterable[HostThroughput] = {
    val throughput: mutable.HashMap[String, HostThroughput] =
      new mutable.HashMap[String, HostThroughput]()
    val allFlows = idToFlow.values().asScala
    allFlows.foreach(flow => {
      throughput.getOrElseUpdate(flow.source, new HostThroughput(flow.source)).output += flow.getFlowSize()
      if(flow.destClient != null) {
        throughput.getOrElseUpdate(flow.destClient.host,
          new HostThroughput(flow.destClient.host)).input += flow.getFlowSize()
      }
    })
    throughput.values
  }

  def getFlowInfos(flowIds: Array[String]): Option[Array[FlowInfo]] = {
    val ret = flowIds.map(idToFlow.get(_))
    ret.foreach(v => {
      if (v == null)
        return None
    })
    Some(ret)
  }

  def getFlowInfo(flowId: String): Option[FlowInfo] = {
    val ret = idToFlow.get(flowId)
    if (ret == null) None else Some(ret)
  }

  def contains(flowId: String) = idToFlow.containsKey(flowId)

  /**
   * Calculate remaining time based on remaining flow size
   */ 
  def calcRemainingMillis(sBpsFree: Map[String, Double], rBpsFree: Map[String, Double]): Double = {
    val sBytes = new HashMap[String, Double]().withDefaultValue(0.0)
    val rBytes = new HashMap[String, Double]().withDefaultValue(0.0)

    getFlows.foreach { flowInfo =>
      // FIXME: Assuming a single source and destination for each flow
      val src = flowInfo.source
      val dst = flowInfo.destClient.host
      
      sBytes(src) = sBytes(src) + flowInfo.bytesLeft
      rBytes(dst) = rBytes(dst) + flowInfo.bytesLeft
    }

    // Scale by available capacities
    for ((src, v) <- sBytes) {
      sBytes(src) = v * 8.0 / sBpsFree(src)
    }
    for ((dst, v) <- rBytes) {
      rBytes(dst) = v * 8.0 / rBpsFree(dst)
    }

    math.max(getMaxValue(sBytes.values), getMaxValue(rBytes.values)) * 1000
  }

  /**
   * Calculating alpha for the coflow based on remaining flow size
   */
  def calcAlpha(): Double = {
    val sBytes = new HashMap[String, Double]().withDefaultValue(0.0)
    val rBytes = new HashMap[String, Double]().withDefaultValue(0.0)

    val flows = getFlows
    flows.foreach { flowInfo =>
      // FIXME: Assuming a single source and destination for each flow
      val src = flowInfo.source
      val dst = flowInfo.destClient.host
      sBytes(src) = sBytes(src) + flowInfo.bytesLeft
      rBytes(dst) = rBytes(dst) + flowInfo.bytesLeft
    }
    math.max(getMaxValue(sBytes.values), getMaxValue(rBytes.values))
  }

  private def getMaxValue(values: Iterable[Double]): Double = {
    if (values.isEmpty) 0.0 else values.max
  }

  def addFlow(flowDesc: FlowDescription) {
    if (idToFlow.containsKey(flowDesc.id)) {
      logWarning(s"flow id[${flowDesc.id} already exist in coflow[id = $id,name=${desc.name}],replace it")
    }
    idToFlow.put(flowDesc.id, new FlowInfo(flowDesc))
    bytesLeft_.getAndAdd(flowDesc.sizeInBytes)
  }

  /**
   * Adds destination for a given piece of data. 
   * Assume flowId already exists in idToFlow
   * Returns true if the coflow is ready to go
   */
  def addDestination(flowId: String, destClient: ClientInfo): Boolean = {
    if (idToFlow.get(flowId).destClient == null) {
      numRegisteredFlows.getAndIncrement
    }
    idToFlow.get(flowId).setDestination(destClient)
    postProcessIfReady
  }

  /**
   * Mark this coflow as RUNNING only after all flows are alive
   * Returns true if the coflow is ready to go
   */
  private def postProcessIfReady(): Boolean = {
    if (numRegisteredFlows.get == desc.maxFlows) {
      origAlpha = calcAlpha()
      changeState(CoflowState.READY)
      readyTime = System.currentTimeMillis
      true
    } else {
      false
    }
  }

  /**
   * Keep track of allocation over time
   */
  def setCurrentAllocation(newTotalBps: Double) = {
    allocationOverTime += ((System.currentTimeMillis, newTotalBps))
  }

  def currentAllocation(): (Long, Double) = allocationOverTime.last

  /**
   * Adds destinations for a multiple pieces of data. 
   * Assume flowId already exists in idToFlow
   * Returns true if the coflow is ready to go
   */
  def addDestinations(flowIds: Array[String], destClient: ClientInfo): Boolean = {
    flowIds.foreach { flowId => 
      if (idToFlow.get(flowId).destClient == null) {
        numRegisteredFlows.getAndIncrement
      }
      idToFlow.get(flowId).setDestination(destClient)
    }
    postProcessIfReady
  }

  /**
   * Updates bytes remaining in the specified flow
   * Returns true if the flow has completed; false otherwise
   */
  def updateFlow(flowId: String, bytesSinceLastUpdate: Long, isCompleted: Boolean): Boolean = {
    val flow = idToFlow.get(flowId)
    flow.decreaseBytes(bytesSinceLastUpdate)
    bytesLeft_.getAndAdd(-bytesSinceLastUpdate)
    if (isCompleted) {
      if(flow.bytesLeft != 0) {
        logWarning(s"flow[id = $flowId] in coflow[id=$id,name=${desc.name}] " +
          s"complete,but it has left bytes[${flow.bytesLeft}]")
      }
      numCompletedFlows.getAndIncrement
      true
    }
    false
  }

  def removeFlow(flowId: String) {
    // TODO: 
  }

  def incrementRetryCount = {
    _retryCount += 1
    _retryCount
  }

  def markFinished(endState: CoflowState.Value) {
    changeState(endState)
    endTime = System.currentTimeMillis()
  }

  /**
   * Returns an estimation of remaining bytes
   */
  def remainingSizeInBytes(): Double = {
    bytesLeft.toDouble
  }

  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }
  
  override def toString: String = "CoflowInfo(" + id + "[" + desc + "], state=" + curState + 
    ", numRegisteredFlows=" + numRegisteredFlows.get + ", numCompletedFlows=" + 
    numCompletedFlows.get + ", bytesLeft= " + bytesLeft + ", deadlineMillis= " + 
    desc.deadlineMillis + ")"
}

private[varys] class HostThroughput(val host: String) {
  var input: Long = 0
  var output: Long = 0
}