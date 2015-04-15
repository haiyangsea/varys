package varys.framework.network.netty.message;

import io.netty.buffer.ByteBuf;
import varys.framework.network.netty.buffer.FileSegmentManagedBuffer;
import varys.framework.network.netty.buffer.ManagedBuffer;
import varys.framework.network.netty.protocol.Encodable;
import varys.framework.network.netty.protocol.Encoders;
import varys.framework.network.netty.util.TransportConf;

import java.io.File;
import java.util.*;

/**
 * Created by Allen on 2015/4/14.
 */
public class FlowRequestArray implements Encodable {

  public final FlowRequest[] requests;
  public final int length;
  private final Map<String, FlowRequest> requestMap;

  public FlowRequestArray(FlowRequest[] requests) {
    this.requests = requests;
    this.length = requests.length;
    requestMap = new HashMap<String, FlowRequest>(this.length);
    for (int i = 0; i < this.length; i++) {
      requestMap.put(requests[i].flowId, requests[i]);
    }
  }

  public FlowRequest getRequest(String flowId) {
    return requestMap.get(flowId);
  }

  public FlowRequest get(int i) {
    return requests[i];
  }

  public byte getFlowKind() {
    if (requests[0] instanceof FileFlowRequest) {
      return 1;
    } else {
      return 0;
    }
  }

  public Iterator<ManagedBuffer> toManagedBuffer(TransportConf conf) {
    List<ManagedBuffer> buffers = new ArrayList<ManagedBuffer>(length);
    if (getFlowKind() == 1) {
      for (int i = 0; i < length; i++) {
        FileFlowRequest request = ((FileFlowRequest)requests[i]);
        buffers.add(new FileSegmentManagedBuffer(conf,
                new File(request.path), request.offset, request.size));
      }
    }
    return buffers.iterator();
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
    buf.writeByte(getFlowKind());
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
