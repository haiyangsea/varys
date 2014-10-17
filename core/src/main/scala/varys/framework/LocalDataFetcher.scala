package varys.framework

import varys.framework.client.FetchFlowListener
import varys.{Logging, Utils}

/**
 * Created by hWX221863 on 2014/10/14.
 */
class LocalDataFetcher(
    flow: FlowDescription,
    listener: FetchFlowListener)
  extends Runnable with Logging{

  override def run(): Unit = {
    val desc = flow.asInstanceOf[FileDescription]
    logDebug("Data[%s] is in local file system,just read it directly".format(desc.pathToFile))
    val data = Utils.readFileUseNIO(desc)
    listener.onComplete(flow.id, flow.coflowId, data)
  }
}
