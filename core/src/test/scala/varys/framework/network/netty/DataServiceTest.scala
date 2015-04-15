package varys.framework.network.netty

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Paths, Files}

import scala.collection.JavaConversions._

import org.scalatest.FunSuite
import varys.framework.network.netty.buffer.ManagedBuffer
import varys.framework.network.{FlowFetchingListener, DataService}
import varys.framework.network.netty.message.{FlowRequest, FlowRequestArray, FileFlowRequest}

/**
 * Created by hWX221863 on 2015/4/15.
 */
class DataServiceTest extends FunSuite {
  test("data service") {
    val service = DataService.getDataService
    val server = service.getServer
    server.start()
    println(s"server start at host = ${server.host}, port = ${server.port}")
    val client = service.getClient(server.host, server.port, 0)
    val filename = new File(getClass.getClassLoader.getResource("2.txt").getFile).getAbsolutePath
    val path = Paths.get(filename)

    val content = Files.readAllLines(path, Charset.forName("UTF-8")).mkString(System.lineSeparator())

    val requests = Array(new FileFlowRequest("fatal-flow-id",
      filename, 0, Files.size(path)))

    client.fetchData("fatal",
      new FlowRequestArray(requests.map(_.asInstanceOf[FlowRequest])),
      new FlowFetchingListener {
        override def onFlowFetchSuccess(coflowId: String, flowId: String, data: ManagedBuffer): Unit = {
          assert(coflowId == "fatal")
          assert(flowId == "fatal-flow-id")
          val buffer = data.nioByteBuffer()
          val bytes: Array[Byte] = if (buffer.isDirect) {
            val dst = new Array[Byte](buffer.remaining())
            buffer.get(dst)
            dst
          } else {
            buffer.array()
          }
          assert(new String(bytes, "UTF-8") == content)
          println("get success message")
        }

        override def onFlowFetchFailure(coflowId: String, flowId: String, exception: Throwable): Unit = {
          fail(exception)
        }
      })

    Thread.sleep(2000)

    server.stop()
    client.close()
  }

  test("throttle #1") {
    val service = DataService.getDataService
    val server = service.getServer
    server.start()
    println(s"server start at host = ${server.host}, port = ${server.port}")
    val client = service.getClient(server.host, server.port, 0)
    val filename = new File(getClass.getClassLoader.getResource("2.txt").getFile).getAbsolutePath
    val path = Paths.get(filename)

    val content = Files.readAllLines(path, Charset.forName("UTF-8")).mkString(System.lineSeparator())

    val requests = Array(new FileFlowRequest("fatal-flow-id",
      filename, 0, Files.size(path)))

    client.fetchData("fatal",
      new FlowRequestArray(requests.map(_.asInstanceOf[FlowRequest])),
      new FlowFetchingListener {
        override def onFlowFetchSuccess(coflowId: String, flowId: String, data: ManagedBuffer): Unit = {
          fail("it will not be into this block!")
        }

        override def onFlowFetchFailure(coflowId: String, flowId: String, exception: Throwable): Unit = {
          fail(exception)
        }
      })

    var i = 0L;
    while (i < Int.MaxValue) {
      i += 1
    }


    server.stop()
    client.close()
  }

  test("throttle #2") {
    val service = DataService.getDataService
    val server = service.getServer
    server.start()

    val client = service.getClient(server.host, server.port, 0)
    val filename = new File(getClass.getClassLoader.getResource("2.txt").getFile).getAbsolutePath
    val path = Paths.get(filename)

    val initCoflowId = "fatal-flow-id"
    val initFlowId = "fatal"
    val initString = Files.readAllLines(path, Charset.forName("UTF-8")).mkString(System.lineSeparator())

    val requests = Array(new FileFlowRequest(initFlowId,
      filename, 0, Files.size(path)))

    var receivedFlowId: String = null
    var receivedCoflowId: String = null
    var receivedString: String = null

    client.fetchData(initCoflowId,
      new FlowRequestArray(requests.map(_.asInstanceOf[FlowRequest])),
      new FlowFetchingListener {
        override def onFlowFetchSuccess(coflowId: String, flowId: String, data: ManagedBuffer): Unit = {
          receivedCoflowId = coflowId
          receivedFlowId = flowId
          val buffer = data.nioByteBuffer()
          val bytes: Array[Byte] = if (buffer.isDirect) {
            val dst = new Array[Byte](buffer.remaining())
            buffer.get(dst)
            dst
          } else {
            buffer.array()
          }
          receivedString = new String(bytes, "UTF-8")
        }

        override def onFlowFetchFailure(coflowId: String, flowId: String, exception: Throwable): Unit = {
          fail(exception)
        }
      })

    Thread.sleep(2000)
    println("before set rate")

    assert(receivedString == null)
    assert(receivedCoflowId == null)
    assert(receivedString == null)

    client.updateRate(Double.MaxValue)
    Thread.sleep(5000)

    assert(receivedFlowId == initFlowId)
    assert(receivedCoflowId == initCoflowId)
    assert(receivedString == initString)

    server.stop()
    client.close()
  }
  
  
  test("multiple files") {
    val service = DataService.getDataService
    val server = service.getServer
    server.start()

    val client = service.getClient(server.host, server.port, 100000)

    val metas = (1 to 3).map { i => 
      val name = new File(getClass.getClassLoader.getResource(s"$i.txt").getFile).getAbsolutePath
      val path = Paths.get(name)
      val content = Files.readAllLines(path, Charset.forName("UTF-8")).mkString(System.lineSeparator())
      val flowId = "flow" + i
      val size = Files.size(path)
      (flowId, Meta(name, flowId, content, size))
    }.toMap
    
    val coflow = "coflow"
    
    val requests = metas.values.map(meta =>
      new FileFlowRequest(meta.flowId, meta.path, 0, meta.size)).toArray[FlowRequest]

    client.fetchData(coflow, new FlowRequestArray(requests),  new FlowFetchingListener {
      override def onFlowFetchSuccess(coflowId: String, flowId: String, data: ManagedBuffer): Unit = {
        assert(metas.contains(flowId))
        assert(coflowId == coflow)
        val buffer = data.nioByteBuffer()
        val bytes: Array[Byte] = if (buffer.isDirect) {
          val dst = new Array[Byte](buffer.remaining())
          buffer.get(dst)
          dst
        } else {
          buffer.array()
        }
        val content = new String(bytes, "UTF-8")
        assert (metas(flowId).content == content)
        println(content)
      }

      override def onFlowFetchFailure(coflowId: String, flowId: String, exception: Throwable): Unit = {
        fail(exception)
      }
    })
    Thread.sleep(3000)
    client.close()
    server.stop()
  }
  
  case class Meta(path: String, flowId: String, content: String, size: Long)
}


