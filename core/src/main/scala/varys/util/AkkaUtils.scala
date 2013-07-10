package varys.util

import akka.actor.{ActorRef, Props, ActorSystemImpl, ActorSystem}
import com.typesafe.config.ConfigFactory
import akka.util.duration._
import akka.pattern.ask
import akka.remote.RemoteActorRefProvider
import cc.spray.Route
import cc.spray.io.IoWorker
import cc.spray.{SprayCanRootService, HttpService}
import cc.spray.can.server.HttpServer
import cc.spray.io.pipelines.MessageHandlerDispatch.SingletonHandler
import akka.dispatch.Await
import varys.VarysException
import java.util.concurrent.TimeoutException

/**
 * Various utility classes for working with Akka.
 */
private[varys] object AkkaUtils {

  val AKKA_TIMEOUT_MS: Int = System.getProperty("varys.akka.timeout", "3000").toInt

  /**
   * Creates an ActorSystem ready for remoting, with various Varys features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   *
   * Note: the `name` parameter is important, as even if a client sends a message to right
   * host + port, if the system name is incorrect, Akka will drop the message.
   */
   def createActorSystem(name: String, host: String, port: Int): (ActorSystem, Int) = {
     val akkaThreads = System.getProperty("varys.akka.threads", "4").toInt
     val akkaBatchSize = System.getProperty("varys.akka.batchSize", "15").toInt
     val akkaTimeout = System.getProperty("varys.akka.timeout", "60").toInt
     val akkaFrameSize = System.getProperty("varys.akka.frameSize", "10").toInt
     val logLevel = System.getProperty("varys.akka.logLevel", "DEBUG")
     val lifecycleEvents = if (System.getProperty("varys.akka.logLifecycleEvents", "false").toBoolean) "on" else "off"
     val logRemoteEvents = if (System.getProperty("varys.akka.logRemoteEvents", "true").toBoolean) "on" else "off"
     val akkaWriteTimeout = System.getProperty("varys.akka.writeTimeout", "30").toInt

     val akkaConf = ConfigFactory.parseString("""
       akka {
         daemonic = on
         event-handlers = ["akka.event.slf4j.Slf4jEventHandler"]
         extensions = ["com.romix.akka.serialization.kryo.KryoSerializationExtension$"]
         
         actor {
           provider = "akka.remote.RemoteActorRefProvider"

           serializers {  
               java = "akka.serialization.JavaSerializer"
               kryo = "com.romix.akka.serialization.kryo.KryoSerializer"
           }

           serialization-bindings {
             "varys.framework.FrameworkMessage" = kryo
             "java.io.Serializable" = java
           }

           # Details of configuration params is at https://github.com/romix/akka-kryo-serialization
           kryo {
             type = "graph"  
             idstrategy = "incremental"  

             # Define a default size for serializer pool
             # Try to define the size to be at least as big as the max possible number
             # of threads that may be used for serialization, i.e. max number
             # of threads allowed for the scheduler
             serializer-pool-size = 32

             # Define a default size for byte buffers used during serialization
             buffer-size = 65536  

             use-manifests = false
             implicit-registration-logging = true 
             kryo-trace = true

             classes = [  
               "varys.framework.RegisterSlave",  
               "varys.framework.Heartbeat",
               "varys.framework.RegisteredSlave",
               "varys.framework.RegisterSlaveFailed",
               "varys.framework.RegisterClient",
               "varys.framework.RegisterCoflow",
               "varys.framework.UnregisterCoflow",
               "varys.framework.RequestBestRxMachines",
               "varys.framework.RequestBestTxMachines",
               "varys.framework.RegisteredClient",
               "varys.framework.CoflowKilled",
               "varys.framework.RegisterClientFailed",
               "varys.framework.RegisteredCoflow",
               "varys.framework.UnregisteredCoflow",
               "varys.framework.BestRxMachines",
               "varys.framework.BestTxMachines",
               "varys.framework.UpdatedRates",
               "varys.framework.AddFlow",
               "varys.framework.AddFlows",
               "varys.framework.GetFlow",
               "varys.framework.GetFlows",
               "varys.framework.FlowProgress",
               "varys.framework.DeleteFlow",
               "varys.framework.GotFlowDesc",
               "varys.framework.GotFlowDescs",
               "varys.framework.CoflowDescription",
               "varys.framework.CoflowType$",
               "varys.framework.FlowDescription",
               "varys.framework.DataIdentifier",
               "varys.framework.DataType$",
               "scala.collection.immutable.Map$Map1"
             ]  
           }
         }
       }
       
       akka.loglevel = "%s"
       akka.stdout-loglevel = "%s"
       akka.remote.transport = "akka.remote.netty.NettyRemoteTransport"
       akka.remote.netty.hostname = "%s"
       akka.remote.netty.port = %d
       akka.remote.netty.connection-timeout = %ds
       akka.remote.netty.message-frame-size = %d MiB
       akka.remote.netty.execution-pool-size = %d
       akka.actor.default-dispatcher.throughput = %d
       akka.remote.log-remote-lifecycle-events = %s
       akka.remote.log-sent-messages = %s
       akka.remote.log-received-messages = %s
       akka.remote.netty.write-timeout = %ds
       """.format(logLevel, logLevel, host, port, akkaTimeout, akkaFrameSize, akkaThreads, akkaBatchSize,
         lifecycleEvents, logRemoteEvents, logRemoteEvents, akkaWriteTimeout))

     val actorSystem = ActorSystem(name, akkaConf)

     // Figure out the port number we bound to, in case port was passed as 0. This is a bit of a
     // hack because Akka doesn't let you figure out the port through the public API yet.
     val provider = actorSystem.asInstanceOf[ActorSystemImpl].provider
     val boundPort = provider.asInstanceOf[RemoteActorRefProvider].transport.address.port.get
     return (actorSystem, boundPort)
   }

  /**
   * Creates a Spray HTTP server bound to a given IP and port with a given Spray Route object to
   * handle requests. Returns the bound port or throws a VarysException on failure.
   */
  def startSprayServer(actorSystem: ActorSystem, ip: String, port: Int, route: Route, 
      name: String = "HttpServer"): ActorRef = {
    val ioWorker = new IoWorker(actorSystem).start()
    val httpService = actorSystem.actorOf(Props(new HttpService(route)))
    val rootService = actorSystem.actorOf(Props(new SprayCanRootService(httpService)))
    val server = actorSystem.actorOf(
      Props(new HttpServer(ioWorker, SingletonHandler(rootService))), name = name)
    actorSystem.registerOnTermination { ioWorker.stop() }
    val timeout = 3.seconds
    val future = server.ask(HttpServer.Bind(ip, port))(timeout)
    try {
      Await.result(future, timeout) match {
        case bound: HttpServer.Bound =>
          return server
        case other: Any =>
          throw new VarysException("Failed to bind web UI to port " + port + ": " + other)
      }
    } catch {
      case e: TimeoutException =>
        throw new VarysException("Failed to bind web UI to port " + port)
    }
  }
  
  /** 
   * Send a one-way message to an actor, to which we expect it to reply with true. 
   */
  def tellActor(actor: ActorRef, message: Any) {
    if (!askActorWithReply[Boolean](actor, message)) {
      throw new VarysException(actor + " returned false, expected true.")
    }
  }

  /**
   * Send a message to an actor and get its result within a default timeout, or
   * throw a VarysException if this fails.
   */
  def askActorWithReply[T](actor: ActorRef, message: Any, timeout: Int = AKKA_TIMEOUT_MS): T = {
    if (actor == null) {
      throw new VarysException("Error sending message as the actor is null " +
        "[message = " + message + "]")
    }
    
    try {
      val future = actor.ask(message)(timeout.millis)
      val result = Await.result(future, timeout.millis)
      if (result == null) {
        throw new Exception(actor + " returned null")
      }
      return result.asInstanceOf[T]
    } catch {
      case ie: InterruptedException => throw ie
      case e: Exception => {
        throw new VarysException(
          "Error sending message to " + actor + " [message = " + message + "]", e)
      }
    }
  }
}
