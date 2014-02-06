/*
 * Copyright 2012 David Crosson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dummy

import org.junit.runner.RunWith
import org.scalatest.FunSuite
//import org.scalatest.ShouldMatchers
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

import akka.actor._
import ActorDSL._
import fr.janalyse.primes._

@RunWith(classOf[JUnitRunner])
class ActorsTest extends FunSuite with ShouldMatchers {

  test("Simple test") {

    implicit val system: ActorSystem = ActorSystem("demo1")

    case class Ask(value: BigInt)
    case class Got(value: BigInt, isPrime: Boolean)

    class PrimeActor extends Actor {
      private val pgen = new PrimesGenerator[BigInt]
      def receive = {
        case Ask(value) => 
          sender ! Got(value, pgen.isPrime(value))
          if (value==17) throw new Exception("internal failure")
      }
    }
    
    val pact1 = system.actorOf(Props[PrimeActor], "PrimeActor1")
    
    implicit val mybox = inbox()    
    pact1 ! Ask(123)
    pact1 ! Ask(17)
    pact1 ! Ask(1001)
    println(mybox.receive())
    println(mybox.receive())
    println(mybox.receive())
  }

  
  
  test("Simple test revisited") {

    implicit val system: ActorSystem = ActorSystem("demo2")

    case class Ask(value: BigInt)
    case class Got(value: BigInt, isPrime: Boolean)

    class PrinterActor extends Actor {
      def receive = {
        case Got(value, true) => println(s"$value is prime") 
        case Got(value, false) => println(s"$value is not prime")
      }
    }

    class CheckerActor(printer:ActorRef) extends Actor {
      private val pgen = new PrimesGenerator[BigInt]
      def receive = {
        case Ask(value) => printer ! Got(value, pgen.isPrime(value))
      }
    }
    
    val printer = actor("PrinterActor1") {new PrinterActor}
    val checker = actor("PrimeActor1") {new CheckerActor(printer)}
    
    checker ! Ask(123)
    checker ! Ask(1299709)
    checker ! Ask(17)
    checker ! Ask(1001)
  }

}
