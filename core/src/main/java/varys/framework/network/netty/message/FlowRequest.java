package varys.framework.network.netty.message;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import varys.framework.network.netty.protocol.Encodable;
import varys.framework.network.netty.protocol.Encoders;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * Created by hWX221863 on 2015/4/14.
 */
public class FlowRequest implements Encodable
{
  public final String flowId;
  public final long size;

  public FlowRequest(String flowId, long size) {
    this.flowId = flowId;
    this.size = size;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(flowId, size);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("flowId", flowId)
            .add("size", size)
            .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof OpenFlows) {
      FlowRequest o = (FlowRequest) other;
      return Objects.equal(flowId, o.flowId)
              && size == o.size;
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(flowId) + 8;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, flowId);
    buf.writeLong(size);
  }

  public static FlowRequest decode(ByteBuf buf) {
    String flowId = Encoders.Strings.decode(buf);
    long size = buf.readLong();
    return new FlowRequest(flowId, size);
  }
}
