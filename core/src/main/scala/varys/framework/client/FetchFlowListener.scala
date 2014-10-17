package varys.framework.client

import varys.framework.FlowDescription

/**
 * Created by hWX221863 on 2014/10/13.
 */
trait FetchFlowListener {
  def onComplete(flowId: String, coflowId: String, data: Array[Byte])
  
  def onFailure(flowId: String, coflowId: String, length: Long, e: Throwable)
}
