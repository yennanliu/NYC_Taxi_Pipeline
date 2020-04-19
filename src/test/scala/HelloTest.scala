package NYC_Taxi_Pipeline_Test

import org.scalatest.FunSuite
import collection.mutable.Stack

class HelloTest extends FunSuite {

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

  test("A Stack  should pop values in last-in-first-out order") {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    assert(stack.pop() === 2)
    assert(stack.pop() === 1)
  }

}