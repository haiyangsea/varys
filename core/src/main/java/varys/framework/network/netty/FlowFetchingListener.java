package varys.framework.network.netty;

import varys.framework.network.netty.buffer.ManagedBuffer;

/**
 * Created by hWX221863 on 2015/4/14.
 */
public interface FlowFetchingListener
{
  void onFlowFetchSuccess(String flowId, ManagedBuffer data);

  void onFlowFetchFailure(String flowId, Throwable exception);
}
