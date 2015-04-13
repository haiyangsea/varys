package varys

import java.nio.channels.SocketChannel

import com.google.common.io.Files
import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.io._
import java.net._
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.{Locale, Random, UUID}
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import varys.framework.serializer.Serializer

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.io.Source
import scala.Some

import sun.nio.ch.DirectBuffer
import scala.reflect.ClassTag
import varys.framework.FileFlowDescription
import java.nio.channels.FileChannel.MapMode

/**
 * Various utility methods used by Varys.
 */
private object Utils extends Logging {
  
  /** 
   * Serialize an object using Java serialization 
   */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    return bos.toByteArray
  }

  /** 
   * Deserialize an object using Java serialization 
   */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    return ois.readObject.asInstanceOf[T]
  }

  /** 
   * Deserialize an object using Java serialization and the given ClassLoader 
   */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    return ois.readObject.asInstanceOf[T]
  }

  def isAlpha(c: Char): Boolean = {
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
  }

  /**
   * Shuffle the elements of a collection into a random order, returning the
   * result in a new collection. Unlike scala.util.Random.shuffle, this method
   * uses a local random number generator, avoiding inter-thread contention.
   */
  def randomize[T: ClassTag](seq: TraversableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.toArray)
  }

  /**
   * Shuffle the elements of an array into a random order, modifying the
   * original array. Returns the original array.
   */
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
   */
  lazy val localIpAddress: String = findLocalIpAddress()

  private def findLocalIpAddress(): String = {
    val defaultIpOverride = System.getenv("VARYS_LOCAL_IP")
    if (defaultIpOverride != null) {
      defaultIpOverride
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        for (ni <- NetworkInterface.getNetworkInterfaces) {
          for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress && 
              !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {

            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " + addr.getHostAddress +
              " instead (on interface " + ni.getName + ")")
            logWarning("Set VARYS_LOCAL_IP if you need to bind to another address")
            return addr.getHostAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set VARYS_LOCAL_IP if you need to bind to another address")
      }
      address.getHostAddress
    }
  }

  private var customHostname: Option[String] = None

  /**
   * Allow setting a custom host name because when we run on Mesos we need to use the same
   * hostname it reports to the master.
   */
  def setCustomHostname(hostname: String) {
    customHostname = Some(hostname)
  }

  /**
   * Get the local machine's hostname.
   */
  def localHostName(): String = {
    customHostname.getOrElse(InetAddress.getLocalHost.getHostName)
  }

  private[varys] val daemonThreadFactory: ThreadFactory =
    new ThreadFactoryBuilder().setDaemon(true).build()

  /**
   * Wrapper over newCachedThreadPool.
   */
  def newDaemonCachedThreadPool(): ThreadPoolExecutor =
    Executors.newCachedThreadPool(daemonThreadFactory).asInstanceOf[ThreadPoolExecutor]

  /**
   * Wrapper over newFixedThreadPool.
   */
  def newDaemonFixedThreadPool(nThreads: Int): ThreadPoolExecutor =
    Executors.newFixedThreadPool(nThreads, daemonThreadFactory).asInstanceOf[ThreadPoolExecutor]

  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
   */
  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  /** 
   * Return a string containing part of a file from byte 'start' to 'end'. 
   */
  def offsetBytes(path: String, start: Long, end: Long): String = {
    val file = new File(path)
    val length = file.length()
    val effectiveEnd = math.min(length, end)
    val effectiveStart = math.max(0, start)
    val buff = new Array[Byte]((effectiveEnd-effectiveStart).toInt)
    val stream = new FileInputStream(file)

    stream.skip(effectiveStart)
    stream.read(buff)
    stream.close()
    Source.fromBytes(buff).mkString
  }

  def readFileUseNIO(desc: FileFlowDescription): Array[Byte] = {
    val path = desc.pathToFile
    val fileChannel = new RandomAccessFile(path, "r").getChannel
    val buffer = ByteBuffer.allocate(desc.length.toInt)
    try {
      fileChannel.read(buffer, desc.offset)
    } finally {
      fileChannel.close()
    }
    buffer.array()
  }

  def getSerializer: Serializer = {
    val serializerName: String = System.getProperty("varys.framework.serializer",
      "varys.framework.serializer.JavaSerializer")
    Class.forName(serializerName).newInstance().asInstanceOf[Serializer]
  }

  def readFromChannel(channel: SocketChannel): Option[Array[Byte]] = {
    val data = new Array[Byte](1024)
    val buffer = ByteBuffer.wrap(data)
    var length = channel.read(buffer)
    // 远程Channel已经关闭
    if(length == -1) {
      None
    } else {
      val byteStream = new ByteArrayOutputStream()
      while(length != 0) {
        byteStream.write(data, 0, length)
        buffer.clear()
        length = channel.read(buffer)
      }
      logDebug("read data from channel[%s],size is %d".format(channel.getRemoteAddress.toString,
        byteStream.toByteArray.length))
      Some(byteStream.toByteArray)
    }

  }

  def getClassName[T: Manifest](obj: T): String = {
    import scala.reflect.runtime.{universe => ru}
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val cla = rm.reflect(obj)
    cla.symbol.fullName
  }
}