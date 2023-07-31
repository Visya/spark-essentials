package part1recap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Success
import scala.util.Failure

object ScalaRecap extends App {
  //  Futures
  val aFuture = Future {
    // some expensive computation
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I found a $meaningOfLife")
    case Failure(ex) => println(s"I have failed $ex")
  }

  // Partial functions
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 12
    case 2 => 24
    case _ => 999
  }

  // Implicits
  // auto-injected by the compiler
  def methodWithImplicitArg(implicit x: Int) = x + 43
  implicit val implicitInt = 67
  val implicitCall = methodWithImplicitArg

  // Implicit conversions
  // implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }
  implicit def fromStringToPerson(name: String) = Person(name)
  "Bob".greet

  // implicit classes
  implicit class Dog(name: String) {
    def bark = println("Bark")
  }
  "Lassie".bark

  /*
    Where does compiler looks for implicits?
     - local scope
     - imported scope
     - companion objects in the types involved in the method call
  */
}
