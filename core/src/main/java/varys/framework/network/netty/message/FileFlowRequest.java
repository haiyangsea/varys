package varys.framework.network.netty.message;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import varys.framework.network.netty.protocol.Encoders;

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

  public FileFlowRequest(FlowRequest base, String path, long offset) {
    super(base.flowId, base.size);
    this.path = path;
    this.offset = offset;
  }

  @Override
  public int encodedLength() {
    return super.encodedLength() + Encoders.Strings.encodedLength(path) + 8 + 8;
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    Encoders.Strings.encode(buf, path);
    buf.writeLong(offset);
  }

  @Override
  public int hashCode() {
    return super.hashCode() * 41 + Objects.hashCode(path, offset);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("flowId", flowId)
            .add("size", size)
            .add("path",path)
            .add("offset", offset)
            .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof OpenFlows) {
      FileFlowRequest o = (FileFlowRequest) other;
      return super.equals(o) && Objects.equal(path, o.path)
              && offset == o.offset;
    }
    return false;
  }

  public static FileFlowRequest decode(ByteBuf buf) {
    FlowRequest base = FlowRequest.decode(buf);
    String path = Encoders.Strings.decode(buf);
    long offset = buf.readLong();
    return new FileFlowRequest(base, path, offset);
  }
}
