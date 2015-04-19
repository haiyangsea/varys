package varys.examples.local

import java.io.File

import varys.Utils
import varys.framework.{CoflowType, CoflowDescription}
import varys.framework.client.VarysClient

/**
 * Created by Allen on 2015/4/19.
 */
object LocalClusterSender extends App {

  val fileNames = Seq("1.txt", "2.txt")
  val masterUrl = s"varys://${Utils.localHostName()}:1606"

  println("Starting to create client, master url : " + masterUrl)

  val client = new VarysClient("favor", masterUrl)
  client.start()

  val coflow = new CoflowDescription("test-coflow", CoflowType.SHUFFLE, 2, Byte.MaxValue)

  val coflowId = client.registerCoflow(coflow)
  println("coflow id : " + coflowId)

  fileNames.foreach(name => {
    val f = getClassPathFile(name)
    client.putFile(name, f.getAbsolutePath, coflowId, f.length(), 1)
  })

  println("enter any key to stop...")
  System.in.read()
  client.stop()

  def getClassPathFile(name: String): File = {
    new File(getClass.getClassLoader.getResource(name).getFile)
  }
}