package varys.framework.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import varys.framework.network.netty.protocol.Message;
import varys.framework.network.netty.protocol.RpcResponse;

import java.io.IOException;
import java.util.List;


/**
 * Created by hWX221863 on 2015/4/15.
 */
public abstract class Throttle extends SimpleChannelInboundHandler<Message>
{
  private final Logger logger = LoggerFactory.getLogger(Throttle.class);

  private long bytesRead = 0;

  private void addReadBytes(int size) {
    this.bytesRead += size;
  }

  protected long getReadBytes() {
    return bytesRead;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Message request) {
    addReadBytes(request.encodedLength());
    throttle();

    ctx.fireChannelRead(request);
  }

  public abstract void throttle();

  public abstract void updateRate(double newRate);

  public abstract long getTotalSleepTime();
}
