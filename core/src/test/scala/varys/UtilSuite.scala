package varys

import org.scalatest.FunSuite


class UtilSuite extends FunSuite {

  test("get full class name") {
    val str = "hello"
    val tuple2 = ("hello", "world")
    val seq = Seq
    assert(Utils.getClassName(str) == "java.lang.String")
    assert(Utils.getClassName(tuple2) == "scala.Tuple2")
    assert(Utils.getClassName(seq) == "scala.collection.Seq")
  }
}
