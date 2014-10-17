package varys.framework.client

import varys.framework.FlowDescription

/**
 * Created by hWX221863 on 2014/10/13.
 */
trait FetchFlowListener {
  def complete(flowId: String, coflowId: String, data: Array[Byte])
  
  def failure(flowId: String, coflowId: String, length: Long, e: Throwable)
}
