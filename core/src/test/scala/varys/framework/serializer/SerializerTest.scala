package varys.framework.serializer

import org.scalatest.FunSuite

/**
 * Created by hWX221863 on 2015/4/15.
 */
class SerializerTest extends FunSuite {

  test("serializer") {
    val serializer = Serializer.getSerializer
    val data = serializer.serialize(Person("Allen", 20))
    val person = serializer.deserialize[Person](data)

    assert(person.name == "Allen")
    assert(person.age == 20)
  }
}

case class Person(name: String, age: Int)