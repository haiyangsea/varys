package varys.framework.io

/**
 * Created by hWX221863 on 2014/10/17.
 */
case class DataCenterAddress(host: String, port: Int) {
  override def toString = s"$host:$port"

  val inetSocketAddress = new InetSocketAddress(host, port)
}
