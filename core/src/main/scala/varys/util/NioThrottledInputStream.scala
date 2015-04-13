package varys.util

import java.io.{IOException, InputStream}
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import varys.Logging

class NioThrottledInputStream(
   val channel: SocketChannel,
   val ownerName: String,
   val initBitPerSec: Double = 0.0)
  extends InputStream with Logging  {


  val startTime = System.currentTimeMillis()

  val mBPSLock = new Object

  var maxBytesPerSec = (initBitPerSec / 8).toLong
  var bytesRead = 0L
  var totalSleepTime = 0L

  val SLEEP_DURATION_MS = 50L

  if (maxBytesPerSec < 0) {
    throw new IOException("Bandwidth " + maxBytesPerSec + " is invalid")
  }

  override def read(): Int = {
    throttle()
    val buffer = ByteBuffer.allocate(1)
    val length = channel.read(buffer)
    if (length > 0) {
      bytesRead += 1
    }
    buffer.flip()
    buffer.get()
  }

  override def read(b: Array[Byte]): Int = {
    throttle()
    val buffer = ByteBuffer.wrap(b)
    val readLen = channel.read(buffer)
    if (readLen > 0) {
      bytesRead += readLen
    }
    readLen
  }
  // TODO make it better
  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    throttle()
    val buffer = ByteBuffer.wrap(b, off, len)
    val readLen = channel.read(buffer)
    if (readLen > 0) {
      bytesRead += readLen
    }
    readLen
  }

  private def throttle() {
    while (maxBytesPerSec <= 0.0) {
      mBPSLock.synchronized {
        logTrace(this + " maxBytesPerSec <= 0.0. Sleeping.")
        mBPSLock.wait()
      }
    }

    // NEVER exceed the specified rate
    while (getBytesPerSec > maxBytesPerSec) {
      try {
        Thread.sleep(SLEEP_DURATION_MS)
        totalSleepTime += SLEEP_DURATION_MS
      } catch {
        case ie: InterruptedException => throw new IOException("Thread aborted", ie)
      }
    }
  }

  def setNewRate(newMaxBitPerSec: Double) {
    maxBytesPerSec = (newMaxBitPerSec / 8).toLong
    mBPSLock.synchronized {
      logDebug(this + "[newMaxBitPerSec = " + newMaxBitPerSec + "]")
      mBPSLock.notifyAll()
    }
  }

  def getTotalBytesRead() = bytesRead

  def getBytesPerSec(): Long = {
    val elapsed = (System.currentTimeMillis() - startTime) / 1000
    if (elapsed == 0) {
      bytesRead
    } else {
      bytesRead / elapsed
    }
  }

  def getTotalSleepTime() = totalSleepTime

  override def toString(): String = {
    "NioThrottledInputStream{" +
      "ownerName=" + ownerName +
      ", bytesRead=" + bytesRead +
      ", maxBytesPerSec=" + maxBytesPerSec +
      ", bytesPerSec=" + getBytesPerSec +
      ", totalSleepTime=" + totalSleepTime +
      '}';
  }
}
