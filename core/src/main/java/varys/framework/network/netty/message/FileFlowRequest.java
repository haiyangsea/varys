package varys.framework.network.netty.message;

/**
 * Created by hWX221863 on 2015/4/14.
 */
public class FileFlowRequest extends FlowRequest
{
  public final String path;
  public final long offset;

  public FileFlowRequest(String flowId, String path, long offset, long size) {
    super(flowId, size);
    this.path = path;
    this.offset = offset;
  }
}
