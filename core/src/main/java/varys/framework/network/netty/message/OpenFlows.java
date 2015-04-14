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
  public final FlowRequest[] flowIds;

  public OpenFlows(String coflowId, FlowRequest[] flowIds) {
    this.coflowId = coflowId;
    this.flowIds = flowIds;
  }

  @Override
  protected Type type() { return Type.OPEN_FLOWS; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(coflowId)
            + Encoders.StringArrays.encodedLength(flowIds);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, coflowId);
    Encoders.StringArrays.encode(buf, flowIds);
  }

  public static OpenFlows decode(ByteBuf buf) {
    String coflowId = Encoders.Strings.decode(buf);
    String[] flowIds = Encoders.StringArrays.decode(buf);
    return new OpenFlows(coflowId, flowIds);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(coflowId) * 41 + Arrays.hashCode(flowIds);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("coflowId", coflowId)
            .add("flowIds", Arrays.toString(flowIds))
            .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof OpenFlows) {
      OpenFlows o = (OpenFlows) other;
      return Objects.equal(coflowId, o.coflowId)
              && Arrays.equals(flowIds, o.flowIds);
    }
    return false;
  }
}
