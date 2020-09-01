package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = true
  val aBoolean2 = false // Types is optional

  // expression
  val anIfExpr = if(2 > 3) "bigger" else "smaller"

  // instruction vs expression
  // instructions are fundamental building block of imperative languages like java or python
  // instruction are executed one by one and a program is just a sequence of instructions
  // expressions are building block of functional languages like scala
  // expressions are evaluated, they can be reduced to single value

  val theUnit: Unit = println("Hello, Scala") // printing returns Unit, Unit == "No meaningful value", or void in other languages

  // functions
  def myFunction(x: Int) = 42

  // OOP
  class Animal
  class Cat extends Animal
  trait Carnivore {
    def eat(animal: Animal): Unit
  }
  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // singleton pattern
  object MySingleton
  // companions: class + object
  object Carnivore // companion object

  // generics
  trait MyList[A]

  // method notations
  val x = 1 + 2
  val y = 1.+(2)

  // Functional programming
  val incrementer: Function1[Int, Int] = new Function[Int, Int] {
    override def apply(v1: Int): Int = x + 1
  }

  val incrementer2: Function1[Int, Int] = new (Int => Int) {
    override def apply(v1: Int): Int = x + 1
  }

  val incrementer3: Function1[Int, Int] = x => x + 1 // anonymous function or lambda

  val incrementer4: (Int) => Int = x => x + 1

  val incremented = incrementer(42) // same as incrementer.apply(42)

  val processedList = List(1, 2, 3).map(incrementer)

  // pattern matching
  val unknow: Any = 42
  val ordinal = unknow match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case e: NullPointerException => "some returned values"
    case _ =>  "something else"
  }


  // Future
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    // some expensive computation on another thread
    42
  }
  aFuture.onComplete {
    case Success(value) => println(s"I found a meaning of life: $value")
    case Failure(ex) => println("I have failed")
  }

  // Partial function
  val aNotPartialFunction = (x: Int) => x match {
    case 1 => 42
    case 8 => 56
    case _ => 999
  }

  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 42
    case 8 => 56
    case _ => 999
  }

  // Implicits
  // - auto-injected by the compiler
  def methodWithImplicitArgument(implicit  x: Int) = x + 43
  implicit val implicitVal: Int = 67
  val implicitCall = methodWithImplicitArgument // compiler will add implicit argument 67

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet(): Unit = println(s"Hi, my name is $name")
  }

  implicit def fromStringToPerson(name: String): Person = Person(name)
  "Bob".greet() //fromStringToPerson("Bob").greet()

  // implicit conversions - implicit classes
  implicit class Dog(name: String) {
    def bark() = println("Bark!")
  }

  "Lassie".bark()

  /*
   How implicit is found:
    - local scope
    - imported scope (import scala.concurrent.ExecutionContext.Implicits.global)
    - companion objects of types involved in the method call
   */



}
