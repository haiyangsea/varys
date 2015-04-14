package varys.framework.network

import java.nio.ByteBuffer

import varys.framework.{FlowDescription, FileFlowDescription, ObjectFlowDescription}

/**
 * Created by Allen on 2015/4/13.
 */
trait DataServer {
  def putObjectData(desc: ObjectFlowDescription, data: ByteBuffer): Unit

  def start(): Unit

  def stop(): Unit

  def host: String

  def port: Int
}
