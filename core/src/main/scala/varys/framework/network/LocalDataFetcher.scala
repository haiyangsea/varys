package varys.framework.network

import java.io.File

import varys.framework.network.netty.NettyDataService
import varys.framework.network.netty.buffer.FileSegmentManagedBuffer
import varys.framework.{FileFlowDescription, FlowDescription}
import varys.Logging

/**
 * Created by hWX221863 on 2014/10/14.
 */
class LocalDataFetcher(
    flow: FlowDescription,
    listener: FlowFetchingListener)
  extends Runnable with Logging{

  override def run(): Unit = {
    try {
      val desc = flow.asInstanceOf[FileFlowDescription]
      logDebug("Data[%s] is in local file system,just read it directly".format(desc.path))
      val data = new FileSegmentManagedBuffer(NettyDataService.conf,
        new File(desc.path), desc.offset, desc.length)
      listener.onFlowFetchSuccess(flow.coflowId, flow.id, data)
    } catch {
      case e: Throwable => listener.onFlowFetchFailure(flow.coflowId, flow.id, e)
    }
  }
}
