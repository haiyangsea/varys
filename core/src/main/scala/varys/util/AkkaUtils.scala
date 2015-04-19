package varys.util

import akka.actor._
import akka.pattern.ask

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.Await

import varys.VarysException

/**
 * Various utility classes for working with Akka.
 */
private[varys] object AkkaUtils {

  val AKKA_TIMEOUT_MS: Int = System.getProperty("varys.akka.timeout", "30").toInt * 1000

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
    val akkaFrameSize = System.getProperty("varys.akka.frameSize", "10").toInt * 1024 * 1024
    val logLevel = System.getProperty("varys.akka.logLevel", "ERROR")
    val lifecycleEvents = if (System.getProperty("varys.akka.logLifecycleEvents", "false").toBoolean) "on" else "off"
    //    val logRemoteEvents = if (System.getProperty("varys.akka.logRemoteEvents", "false").toBoolean) "on" else "off"
    //    val akkaWriteTimeout = System.getProperty("varys.akka.writeTimeout", "30").toInt

    val akkaConf = ConfigFactory.parseString(
      s"""
      |akka.daemonic = on
      |akka.loggers = [""akka.event.slf4j.Slf4jLogger""]
      |akka.stdout-loglevel = $logLevel
      |akka.jvm-exit-on-fatal-error = off
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = $port
      |akka.remote.netty.tcp.tcp-nodelay = on
      |akka.remote.netty.tcp.connection-timeout = $akkaTimeout s
      |akka.remote.netty.tcp.maximum-frame-size = ${akkaFrameSize}B
      |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
      |akka.actor.default-dispatcher.throughput = $akkaBatchSize
      |akka.remote.log-remote-lifecycle-events = $lifecycleEvents
      |akka.log-dead-letters = $lifecycleEvents
      |akka.log-dead-letters-during-shutdown = $lifecycleEvents
      """.stripMargin)

    val actorSystem = ActorSystem(name, akkaConf)

    // Figure out the port number we bound to, in case port was passed as 0. This is a bit of a
    // hack because Akka doesn't let you figure out the port through the public API yet.
    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.getDefaultAddress.port.get
    return (actorSystem, boundPort)
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
      throw new VarysException("Error sending message as the actor is null " + "[message = " + 
        message + "]")
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

  def getActorRef(url: String, context: ActorContext): ActorRef = {
    val timeout = AKKA_TIMEOUT_MS.millis
    Await.result(context.actorSelection(url).resolveOne(timeout), timeout)
  }  
}
