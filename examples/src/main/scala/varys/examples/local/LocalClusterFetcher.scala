package varys.examples.local

import java.nio.ByteBuffer
import java.util.concurrent.Executors

import varys.Utils
import varys.framework.client.VarysClient

/**
 * Created by Allen on 2015/4/19.
 */
object LocalClusterFetcher extends App {
  val executors = Executors.newCachedThreadPool()

  val fileNames = Seq("1.txt", "2.txt")
  val masterUrl = s"varys://${Utils.localHostName()}:1606"

  println("Starting to create client, master url : " + masterUrl)

  val client = new VarysClient("favor", masterUrl, TestClientListener)
  client.start()

  val coflowId = "COFLOW-000000"

  fileNames.foreach { name =>
    executors.execute( new Runnable {
      override def run = {
        val bytes = client.getFile(name, coflowId)
        println(new String(bytes, "UTF-8"))
      }
    })
  }

  println("enter any key to stop...")
  System.in.read()
  client.stop()
  executors.shutdown()
}
