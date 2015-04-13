package varys.framework.network

import java.nio.ByteBuffer

import varys.framework.{FakeFlowDescription, ObjectFlowDescription, FileFlowDescription}

/**
 * Created by Allen on 2015/4/13.
 */
trait DataClient {

  def getFileData(desc: FileFlowDescription): ByteBuffer

  def getObjectData(desc: ObjectFlowDescription): ByteBuffer

  def getFakeData(desc: FakeFlowDescription): ByteBuffer

  def updateRate(rate: Double): Unit
}
