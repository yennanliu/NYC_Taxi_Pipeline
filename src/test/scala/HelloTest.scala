  
package NYC_Taxi_Pipeline_Test

import org.scalatest.FunSuite

class HelloTests extends FunSuite {
  test("the name is set correctly in constructor") {
  	val Person = Map("Kirk" -> "Enterprise", "Picard" -> "Enterprise-D", "Sisko" -> "Deep Space Nine", "Janeway" -> "Voyager")
    val p = Person("Kirk")
    assert(p == "Enterprise")
  }

  test("a Person's name can be changed") {

  	val Person = Map("Kirk" -> "Enterprise", "Picard" -> "Enterprise-D", "Sisko" -> "Deep Space Nine", "Janeway" -> "Voyager")
    val p = Person("Picard")
    assert(p == "Enterprise-D")
  }
}