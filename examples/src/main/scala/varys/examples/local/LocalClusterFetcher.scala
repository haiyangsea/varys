package varys

import java.nio.ByteBuffer

import varys.framework.network.FlowFetchListener
import varys.framework.client.VarysClient

/**
 * Created by Allen on 2015/4/19.
 */
object LocalClusterFetcher extends App {

    val fileNames = Seq("1.txt", "2.txt")
    val masterUrl = s"varys://${Utils.localHostName()}:1606"

    println("Starting to create client, master url : " + masterUrl)

    val client = new VarysClient("favor", masterUrl, TestClientListener)
    client.start()

    val coflowId = "COFLOW-000000"

    client.getFlows(fileNames, coflowId, new FlowFetchListener {
      override def onFlowFetchFailure(coflowId: String, flowId: String, length: Long, exception: Throwable): Unit = {
        println(s"failed: coflow id = $coflowId, flow id = $flowId ")
        exception.printStackTrace()
      }

      override def onFlowFetchSuccess(coflowId: String, flowId: String, data: ByteBuffer): Unit = {
        println(s"get data : coflow id = $coflowId, flow id = $flowId, content = ${getStringFromBuffer(data)}")
      }
    })

    println("enter any key to stop...")
    System.in.read()
    client.stop()


  def getStringFromBuffer(buffer: ByteBuffer): String = {
    val data = if (buffer.isDirect) {
      val bytes = new Array[Byte](buffer.remaining())
      buffer.get(bytes)
      bytes
    } else {
      buffer.array()
    }
    new String(data, "UTF-8")
  }
}
