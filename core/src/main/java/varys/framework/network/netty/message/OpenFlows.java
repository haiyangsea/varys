package varys.framework.network.netty.message;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import varys.framework.FlowDescription;
import varys.framework.network.netty.protocol.Encoders;

import java.util.Arrays;

/**
 * Created by hWX221863 on 2015/4/14.
 */
public class OpenFlows extends FlowTransferMessage
{
  public final String coflowId;
  public final FlowRequestArray flowRequests;

  public OpenFlows(String coflowId, FlowRequestArray flowRequests) {
    this.coflowId = coflowId;
    this.flowRequests = flowRequests;
  }

  @Override
  protected Type type() { return Type.OPEN_FLOWS; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(coflowId)
            + flowRequests.encodedLength();
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, coflowId);
    flowRequests.encode(buf);
  }

  public static OpenFlows decode(ByteBuf buf) {
    String coflowId = Encoders.Strings.decode(buf);
    FlowRequestArray flowRequests = FlowRequestArray.decode(buf);
    return new OpenFlows(coflowId, flowRequests);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(coflowId) * 41 + Arrays.hashCode(flowRequests.requests);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("coflowId", coflowId)
            .add("flowIds", Arrays.toString(flowRequests.requests))
            .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof OpenFlows) {
      OpenFlows o = (OpenFlows) other;
      return Objects.equal(coflowId, o.coflowId)
              && Arrays.equals(flowRequests.requests, o.flowRequests.requests);
    }
    return false;
  }
}
