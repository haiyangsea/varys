package varys.framework.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by hWX221863 on 2015/4/15.
 */
public class RateThrottle extends Throttle
{
  private final Logger logger = LoggerFactory.getLogger(Throttle.class);

  private long bytesRead = 0;
  private long maxBytesPerSec;
  private long totalSleepTime;

  private final long SLEEP_DURATION_MS = 50L;
  private final long startTime = System.currentTimeMillis();

  private final Object mBPSLock = new Object();

  public RateThrottle(double initBitPerSec) {
    maxBytesPerSec = (long)(initBitPerSec / 8);
  }

  public void addReadBytes(int size) {
    this.bytesRead += size;
  }

  public void throttle() {
    try {
      while (maxBytesPerSec <= 0.0) {
        synchronized(mBPSLock) {
          logger.trace(this + " maxBytesPerSec <= 0.0. Sleeping.");
          mBPSLock.wait();
        }
      }

      // NEVER exceed the specified rate
      while (getBytesPerSec() > maxBytesPerSec || maxBytesPerSec <= 0) {
        try {
          Thread.sleep(SLEEP_DURATION_MS);
          totalSleepTime += SLEEP_DURATION_MS;
        } catch (InterruptedException ie) {
          throw new IOException("Thread aborted", ie);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void updateRate(double newRate) {
    maxBytesPerSec = (long)(newRate / 8);
    synchronized(mBPSLock) {
      logger.trace(this + " newMaxBitPerSec = " + newRate);
      mBPSLock.notifyAll();
    }
  }

  private long getBytesPerSec() {
    long elapsed = (System.currentTimeMillis() - startTime) / 1000;
    if (elapsed == 0)
    {
      return bytesRead;
    } else
    {
      return bytesRead / elapsed;
    }
  }
}
