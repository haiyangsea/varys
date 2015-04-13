package varys.framework

private[varys] object DataType extends Enumeration {
  type DataType = Value

  val FAKE, INMEMORY, ONDISK = Value
}

private[varys] case class EndPoint(host: String, port: Int)

private[varys] case class DataIdentifier(
    dataId: String, 
    coflowId: String)

private[varys] case class FlowDescription(
    id: String,  // Expected to be unique within the coflow
    coflowId: String,  // Must be a valid coflow
    dataType: DataType.DataType,  // http://www.scala-lang.org/node/7661
    sizeInBytes: Long,
    maxReceivers: Int,  // Upper-bound on the number of receivers (how long to keep it around?)
    host: String,
    port: Int) {

  val dataId = DataIdentifier(id, coflowId)
  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "FlowDescription(" + id + ":" + dataType + ":" + coflowId + 
    " # " + sizeInBytes + " Bytes)"
}

private[varys] class FileFlowDescription(
    flowId : String,  // Expected to be unique within the coflow
    val pathToFile: String,
    coflowId : String,  // Must be a valid coflow
    dataType : DataType.DataType,
    val offset : Long,
    val length : Long,
    val maxR : Int,
    host: String,
    port: Int)
  extends FlowDescription(flowId, coflowId, dataType, length, maxR, host, port) {

  override def toString: String = "FileDescription(" + id + "["+ pathToFile + "]:" + dataType + 
    ":" + coflowId + " # " + sizeInBytes + " Bytes)"
}

private[varys] class ObjectFlowDescription(
    val id_ : String,  // Expected to be unique within the coflow
    val className: String, 
    val cId_ : String,  // Must be a valid coflow
    val dataType_ : DataType.DataType,
    val serializedSize : Long,
    val maxR_ : Int,
    host: String,
    port: Int)
  extends FlowDescription(id_, cId_, dataType_, serializedSize, maxR_, host, port) {

  override def toString: String = "ObjectDescription(" + id + "["+ className + "]:" + dataType + 
    ":" + coflowId + " # " + sizeInBytes + " Bytes)"
}

private[varys] class FakeFlowDescription(
    id: String,
    cId: String,
    dataType: DataType.DataType,
    dataSize: Long,
    maxR: Int,
    host: String,
    port: Int)
  extends FlowDescription(id, cId, dataType, dataSize, maxR, host, port) {
  
  override def toString: String = s"FakeDataDescription($id : $dataType : $coflowId #$sizeInBytes Bytes)"
}
