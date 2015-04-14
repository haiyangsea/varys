package varys.framework.network.netty.message;

/**
 * Created by hWX221863 on 2015/4/14.
 */
public class FlowRequest
{
  public final String flowId;
  public final long size;

  public FlowRequest(String flowId, long size) {
    this.flowId = flowId;
    this.size = size;
  }
}
