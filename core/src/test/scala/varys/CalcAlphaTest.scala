package varys

import org.scalatest.FunSuite

import scala.collection.mutable.HashMap

/**
 * Created by hWX221863 on 2015/4/20.
 */
class CalcAlphaTest extends FunSuite {
  test("calc alpha") {
    val sBytes = new HashMap[String, Double]().withDefaultValue(0.0)
    val rBytes = new HashMap[String, Double]().withDefaultValue(0.0)
    math.max(sBytes.values.max, rBytes.values.max)
  }
}
