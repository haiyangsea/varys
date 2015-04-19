package varys.framework.network.netty.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import varys.framework.network.netty.message.FlowKind;
import varys.framework.network.netty.message.FlowTransferMessage;

/**
 * Created by hWX221863 on 2015/4/14.
 */
public class StreamHandle extends FlowTransferMessage
{
  public final long streamId;
  public final int numChunks;

  public StreamHandle(long streamId, int numChunks) {
    this.streamId = streamId;
    this.numChunks = numChunks;
  }

  @Override
  protected FlowKind type() { return FlowKind.STREAM_HANDLE; }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId, numChunks);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("streamId", streamId)
            .add("numChunks", numChunks)
            .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof StreamHandle) {
      StreamHandle o = (StreamHandle) other;
      return Objects.equal(streamId, o.streamId)
              && Objects.equal(numChunks, o.numChunks);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return 8 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(streamId);
    buf.writeInt(numChunks);
  }

  public static StreamHandle decode(ByteBuf buf) {
    long streamId = buf.readLong();
    int numChunks = buf.readInt();
    return new StreamHandle(streamId, numChunks);
  }
}

