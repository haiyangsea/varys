package varys.framework.network

import varys.framework.network.netty.buffer.NioManagedBuffer
import varys.framework.{FileFlowDescription, FlowDescription}
import varys.{Logging, Utils}

/**
 * Created by hWX221863 on 2014/10/14.
 */
class LocalDataFetcher(
    flow: FlowDescription,
    listener: FlowFetchingListener)
  extends Runnable with Logging{

  override def run(): Unit = {
    val desc = flow.asInstanceOf[FileFlowDescription]
    logDebug("Data[%s] is in local file system,just read it directly".format(desc.path))
    val data = Utils.readFileUseNIO(desc)
    listener.onFlowFetchSuccess(flow.id, flow.coflowId, new NioManagedBuffer(data))
  }
}
