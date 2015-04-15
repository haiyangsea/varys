package varys.framework.network;

import varys.framework.network.netty.buffer.ManagedBuffer;

/**
 * Created by hWX221863 on 2015/4/14.
 */
public interface FlowFetchingListener
{
  void onFlowFetchSuccess(String coflowId, String flowId, ManagedBuffer data);

  void onFlowFetchFailure(String coflowId, String flowId, Throwable exception);
}
