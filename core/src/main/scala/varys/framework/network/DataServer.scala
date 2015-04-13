package varys.framework.network

import varys.framework.{FlowDescription, FileFlowDescription, ObjectFlowDescription}

/**
 * Created by Allen on 2015/4/13.
 */
trait DataServer {
  def putObjectData(desc: ObjectFlowDescription): Unit

  def putFileData(desc: FileFlowDescription): Unit

  def putFakeData(desc: FlowDescription): Unit

  def start(): Unit

  def stop(): Unit

  def host: String

  def port: Int
}
