package varys.framework.network

import java.nio.ByteBuffer

/**
 * Created by hWX221863 on 2015/4/15.
 */
trait FlowFetchListener {
  def onFlowFetchSuccess(coflowId: String, flowId: String, data: ByteBuffer)

  def onFlowFetchFailure(coflowId: String, flowId: String, length: Long, exception: Throwable)
}
