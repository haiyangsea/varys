package varys.framework.network.netty.message;

import io.netty.buffer.ByteBuf;
import varys.framework.network.netty.protocol.Encodable;
import varys.framework.network.netty.protocol.Encoders;

/**
 * Created by Allen on 2015/4/14.
 */
public class FlowRequestArray implements Encodable {

  public final FlowRequest[] requests;
  public FlowRequestArray(FlowRequest[] requests) {
    this.requests = requests;
  }

  @Override
  public int encodedLength() {
    int length = 1 + 4;
    for (int i = 0; i < requests.length; i++) {
      length += requests[i].encodedLength();
    }
    return length;
  }

  @Override
  public void encode(ByteBuf buf) {
    byte type = 0;
    if (requests[0] instanceof FileFlowRequest) {
      type = 1;
    }
    buf.writeInt(requests.length);

    for (int i = 0; i < requests.length; i++) {
      requests[i].encode(buf);
    }
  }

  public static FlowRequestArray decode(ByteBuf buf) {
    byte type = buf.readByte();
    int count = buf.readInt();
    FlowRequest[] requests = new FlowRequest[count];

    switch(type) {
      case 0:
        for(int i = 0; i < count; i++) {
          requests[i] = FlowRequest.decode(buf);
        }
        break;
      case 1:
        for(int i = 0; i < count; i++) {
          requests[i] = FileFlowRequest.decode(buf);
        }
        break;
    }
    return new FlowRequestArray(requests);
  }
}
