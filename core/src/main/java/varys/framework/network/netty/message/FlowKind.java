package varys.framework.network.netty.message;

/**
 * Created by Allen on 2015/4/19.
 */
public enum FlowKind {
  OPEN_FLOWS(0), UPLOAD_FLOWS(1), STREAM_HANDLE(2);

  private final byte id;

  private FlowKind(int id) {
    assert id < 128 : "Cannot have more than 128 message types";
    this.id = (byte)id;
  }

  public byte id() {return id;}
}
