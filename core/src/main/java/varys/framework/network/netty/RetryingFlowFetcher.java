package varys.framework.network.netty;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import varys.framework.network.FlowFetchingListener;
import varys.framework.network.netty.buffer.ManagedBuffer;
import varys.framework.network.netty.message.FlowRequest;
import varys.framework.network.netty.message.FlowRequestArray;
import varys.framework.network.netty.util.NettyUtils;
import varys.framework.network.netty.util.TransportConf;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by hWX221863 on 2015/4/15.
 */
public class RetryingFlowFetcher {

  /**
   * Used to initiate the first fetch for all blocks, and subsequently for retrying the fetch on any
   * remaining blocks.
   */
  public static interface FlowFetchStarter
  {
    void createAndStart(FlowRequestArray requests, FlowFetchingListener listener) throws IOException;
  }

  /** Shared executor service used for waiting and retrying. */
  private static final ExecutorService executorService = Executors.newCachedThreadPool(NettyUtils.createThreadFactory("Block Fetch Retry"));

  private final Logger logger = LoggerFactory.getLogger(RetryingFlowFetcher.class);

  /** Used to initiate new Block Fetches on our remaining blocks. */
  private final FlowFetchStarter fetchStarter;

  /** Parent listener which we delegate all successful or permanently failed block fetches to. */
  private final FlowFetchingListener listener;

  /** Max number of times we are allowed to retry. */
  private final int maxRetries;

  /** Milliseconds to wait before each retry. */
  private final int retryWaitTime;

  // NOTE:
  // All of our non-final fields are synchronized under 'this' and should only be accessed/mutated
  // while inside a synchronized block.
  /** Number of times we've attempted to retry so far. */
  private int retryCount = 0;

  /**
   * Set of all block ids which have not been fetched successfully or with a non-IO Exception.
   * A retry involves requesting every outstanding block. Note that since this is a LinkedHashSet,
   * input ordering is preserved, so we always request blocks in the same order the user provided.
   */
  private final LinkedHashSet<String> outstandingFlowIds;
  private final FlowRequestArray requests;
  /**
   * The BlockFetchingListener that is active with our current BlockFetcher.
   * When we start a retry, we immediately replace this with a new Listener, which causes all any
   * old Listeners to ignore all further responses.
   */
  private RetryingFlowFetchListener currentListener;

  private String coflowId;
  public RetryingFlowFetcher(
          TransportConf conf,
          FlowFetchStarter fetchStarter,
          String coflowId,
          FlowRequestArray requests,
          FlowFetchingListener listener) {
    this.fetchStarter = fetchStarter;
    this.listener = listener;
    this.maxRetries = conf.maxIORetries();
    this.retryWaitTime = conf.ioRetryWaitTimeMs();
    this.outstandingFlowIds = Sets.newLinkedHashSet();
    for (int i = 0; i < requests.length; i++) {
      this.outstandingFlowIds.add(requests.requests[i].flowId);
    }
    this.requests = requests;
    this.currentListener = new RetryingFlowFetchListener();
    this.coflowId = coflowId;
  }

  /**
   * Initiates the fetch of all blocks provided in the constructor, with possible retries in the
   * event of transient IOExceptions.
   */
  public void start() {
    fetchAllOutstanding();
  }

  /**
   * Fires off a request to fetch all blocks that have not been fetched successfully or permanently
   * failed (i.e., by a non-IOException).
   */
  private void fetchAllOutstanding() {
    // Start by retrieving our shared state within a synchronized block.
    FlowRequest[] flowsToFetch;
    int numRetries;
    RetryingFlowFetchListener myListener;
    synchronized (this) {
      flowsToFetch = new FlowRequest[outstandingFlowIds.size()];
      int i = 0;
      for (String flowId : outstandingFlowIds) {
        flowsToFetch[i] = requests.getRequest(flowId);
        i++;
      }
      numRetries = retryCount;
      myListener = currentListener;
    }

    // Now initiate the fetch on all outstanding blocks, possibly initiating a retry if that fails.
    try {
      fetchStarter.createAndStart(new FlowRequestArray(flowsToFetch), myListener);
    } catch (Exception e) {
      logger.error(String.format("Exception while beginning fetch of %s outstanding blocks %s",
              flowsToFetch.length, numRetries > 0 ? "(after " + numRetries + " retries)" : ""), e);

      if (shouldRetry(e)) {
        initiateRetry();
      } else {
        for (FlowRequest bid : flowsToFetch) {
          listener.onFlowFetchFailure(this.coflowId, bid.flowId, e);
        }
      }
    }
  }

  /**
   * Lightweight method which initiates a retry in a different thread. The retry will involve
   * calling fetchAllOutstanding() after a configured wait time.
   */
  private synchronized void initiateRetry() {
    retryCount += 1;
    currentListener = new RetryingFlowFetchListener();

    logger.info("Retrying fetch ({}/{}) for {} outstanding blocks after {} ms",
            retryCount, maxRetries, outstandingFlowIds.size(), retryWaitTime);

    executorService.submit(new Runnable() {
      @Override
      public void run() {
        Uninterruptibles.sleepUninterruptibly(retryWaitTime, TimeUnit.MILLISECONDS);
        fetchAllOutstanding();
      }
    });
  }

  /**
   * Returns true if we should retry due a block fetch failure. We will retry if and only if
   * the exception was an IOException and we haven't retried 'maxRetries' times already.
   */
  private synchronized boolean shouldRetry(Throwable e) {
    boolean isIOException = e instanceof IOException
            || (e.getCause() != null && e.getCause() instanceof IOException);
    boolean hasRemainingRetries = retryCount < maxRetries;
    return isIOException && hasRemainingRetries;
  }

  /**
   * Our RetryListener intercepts block fetch responses and forwards them to our parent listener.
   * Note that in the event of a retry, we will immediately replace the 'currentListener' field,
   * indicating that any responses from non-current Listeners should be ignored.
   */
  private class RetryingFlowFetchListener implements FlowFetchingListener {
    @Override
    public void onFlowFetchSuccess(String coflowId, String flowId, ManagedBuffer data) {
      // We will only forward this success message to our parent listener if this block request is
      // outstanding and we are still the active listener.
      boolean shouldForwardSuccess = false;
      synchronized (RetryingFlowFetcher.this) {
        if (this == currentListener && outstandingFlowIds.contains(flowId)) {
          outstandingFlowIds.remove(flowId);
          shouldForwardSuccess = true;
        }
      }

      // Now actually invoke the parent listener, outside of the synchronized block.
      if (shouldForwardSuccess) {
        listener.onFlowFetchSuccess(coflowId, flowId, data);
      }
    }

    @Override
    public void onFlowFetchFailure(String coflowId, String flowId, Throwable exception) {
      // We will only forward this failure to our parent listener if this block request is
      // outstanding, we are still the active listener, AND we cannot retry the fetch.
      boolean shouldForwardFailure = false;
      synchronized (RetryingFlowFetcher.this) {
        if (this == currentListener && outstandingFlowIds.contains(flowId)) {
          if (shouldRetry(exception)) {
            initiateRetry();
          } else {
            logger.error(String.format("Failed to fetch block %s, and will not retry (%s retries)",
                    flowId, retryCount), exception);
            outstandingFlowIds.remove(flowId);
            shouldForwardFailure = true;
          }
        }
      }

      // Now actually invoke the parent listener, outside of the synchronized block.
      if (shouldForwardFailure) {
        listener.onFlowFetchFailure(coflowId, flowId, exception);
      }
    }
  }
}

