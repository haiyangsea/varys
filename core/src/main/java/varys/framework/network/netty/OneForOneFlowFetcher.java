package varys.framework.network.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import varys.framework.network.FlowFetchingListener;
import varys.framework.network.netty.buffer.ManagedBuffer;
import varys.framework.network.netty.client.ChunkReceivedCallback;
import varys.framework.network.netty.client.RpcResponseCallback;
import varys.framework.network.netty.client.TransportClient;
import varys.framework.network.netty.message.FlowRequest;
import varys.framework.network.netty.message.FlowRequestArray;
import varys.framework.network.netty.message.FlowTransferMessage;
import varys.framework.network.netty.message.OpenFlows;
import varys.framework.network.netty.protocol.StreamHandle;

import java.util.Arrays;

/**
 * Created by hWX221863 on 2015/4/14.
 */
public class OneForOneFlowFetcher {
  private final Logger logger = LoggerFactory.getLogger(OneForOneFlowFetcher.class);

  private final TransportClient client;
  private final OpenFlows openMessage;
  private final FlowRequestArray requests;
  private final FlowFetchingListener listener;
  private final ChunkReceivedCallback chunkCallback;
  private final String coflowId;

  private StreamHandle streamHandle = null;

  public OneForOneFlowFetcher(
          TransportClient client,
          String coflowId,
          FlowRequestArray requests,
          FlowFetchingListener listener) {
    this.client = client;
    this.openMessage = new OpenFlows(coflowId, requests);
    this.requests = requests;
    this.listener = listener;
    this.chunkCallback = new ChunkCallback();
    this.coflowId = coflowId;
  }

  /** Callback invoked on receipt of each chunk. We equate a single chunk to a single block. */
  private class ChunkCallback implements ChunkReceivedCallback {
    @Override
    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
      // On receipt of a chunk, pass it upwards as a block.
      listener.onFlowFetchSuccess(coflowId, requests.requests[chunkIndex].flowId, buffer);
    }

    @Override
    public void onFailure(int chunkIndex, Throwable e) {
      // On receipt of a failure, fail every block from chunkIndex onwards.
      FlowRequest[] remainingBlockIds = Arrays.copyOfRange(requests.requests, chunkIndex, requests.length);
      failRemainingBlocks(remainingBlockIds, e);
    }
  }

  public void start() {
    if (requests.length == 0) {
      throw new IllegalArgumentException("Zero-sized requests array");
    }

    client.sendRpc(openMessage.toByteArray(), new RpcResponseCallback() {
      @Override
      public void onSuccess(byte[] response) {
        try {
          streamHandle = (StreamHandle) FlowTransferMessage.Decoder.fromByteArray(response);
          logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);

          // Immediately request all chunks -- we expect that the total size of the request is
          // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
          for (int i = 0; i < streamHandle.numChunks; i++) {
            client.fetchChunk(streamHandle.streamId, i, chunkCallback);
          }
        } catch (Exception e) {
          logger.error("Failed while starting block fetches after success", e);
          failRemainingBlocks(requests.requests, e);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        logger.error("Failed while starting block fetches", e);
        failRemainingBlocks(requests.requests, e);
      }
    });
  }

  /** Invokes the "onBlockFetchFailure" callback for every listed block id. */
  private void failRemainingBlocks(FlowRequest[] failedFlows, Throwable e) {
    for (FlowRequest flow : failedFlows) {
      try {
        listener.onFlowFetchFailure(coflowId, flow.flowId, e);
      } catch (Exception e2) {
        logger.error("Error in block fetch failure callback", e2);
      }
    }
  }
}
