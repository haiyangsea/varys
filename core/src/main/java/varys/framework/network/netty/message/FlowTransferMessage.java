package varys.framework.network.netty.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import varys.framework.network.netty.protocol.Encodable;
import varys.framework.network.netty.protocol.StreamHandle;

/**
 * Created by hWX221863 on 2015/4/14.
 */
public abstract class FlowTransferMessage implements Encodable
{
  protected abstract FlowKind type();

  public static class Decoder {
    public static FlowTransferMessage fromByteArray(byte[] msg) {
      ByteBuf buf = Unpooled.wrappedBuffer(msg);
      byte type = buf.readByte();
      switch (type) {
        case 0: return OpenFlows.decode(buf);
        case 1: return UploadFlows.decode(buf);
        case 2: return StreamHandle.decode(buf);
        default: throw new IllegalArgumentException("Unknown message type: " + type);
      }
    }
  }

  public byte[] toByteArray() {
    // Allow room for encoded message, plus the type byte
    ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
    buf.writeByte(type().id());
    encode(buf);
    assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
    return buf.array();
  }
}
