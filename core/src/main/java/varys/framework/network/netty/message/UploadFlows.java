package varys.framework.network.netty.message;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import varys.framework.network.netty.protocol.Encoders;

import java.util.Arrays;

/**
 * Created by hWX221863 on 2015/4/14.
 */
public class UploadFlows extends FlowTransferMessage
{
  public final String appId;
  public final String flowId;
  // TODO: StorageLevel is serialized separately in here because StorageLevel is not available in
  // this package. We should avoid this hack.
  public final byte[] metadata;
  public final byte[] blockData;

  /**
   * @param metadata Meta-information about block, typically StorageLevel.
   * @param blockData The actual block's bytes.
   */
  public UploadFlows(
          String coflowId,
          String flowId,
          byte[] metadata,
          byte[] blockData) {
    this.appId = coflowId;
    this.flowId = flowId;
    this.metadata = metadata;
    this.blockData = blockData;
  }

  @Override
  protected FlowKind type() { return FlowKind.UPLOAD_FLOWS; }

  @Override
  public int hashCode() {
    int objectsHashCode = Objects.hashCode(appId, flowId);
    return (objectsHashCode * 41 + Arrays.hashCode(metadata)) * 41 + Arrays.hashCode(blockData);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("appId", appId)
            .add("flowId", flowId)
            .add("metadata size", metadata.length)
            .add("block size", blockData.length)
            .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof UploadFlows) {
      UploadFlows o = (UploadFlows) other;
      return Objects.equal(appId, o.appId)
              && Objects.equal(flowId, o.flowId)
              && Arrays.equals(metadata, o.metadata)
              && Arrays.equals(blockData, o.blockData);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
            + Encoders.Strings.encodedLength(flowId)
            + Encoders.ByteArrays.encodedLength(metadata)
            + Encoders.ByteArrays.encodedLength(blockData);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, flowId);
    Encoders.ByteArrays.encode(buf, metadata);
    Encoders.ByteArrays.encode(buf, blockData);
  }

  public static UploadFlows decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String blockId = Encoders.Strings.decode(buf);
    byte[] metadata = Encoders.ByteArrays.decode(buf);
    byte[] blockData = Encoders.ByteArrays.decode(buf);
    return new UploadFlows(appId, blockId, metadata, blockData);
  }
}
