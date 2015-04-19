package varys

/**
 * Created by hWX221863 on 2015/4/14.
 */
class VarysConf {

  def get(name: String, default: String): String = {
    System.getProperty(name, default)
  }

  def getInt(name: String, default: Int): Int = {
    get(name, default.toString).toInt
  }
}
