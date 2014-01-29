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
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import reactivemongo.api._
import scala.concurrent.ExecutionContext.Implicits.global
import reactivemongo.bson.{ BSONDocument => BD }
import reactivemongo.bson._
import reactivemongo.core.commands._
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import BSONDocument._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import scala.annotation.tailrec

@RunWith(classOf[JUnitRunner])
class MongoDBTest extends FunSuite with ShouldMatchers with BeforeAndAfter {

  var driver: MongoDriver = _
  var use: MongoConnection = _

  info("----------------------------------------")
  info("Requires mongodb. Starts it using the following command")
  info("$ mkdir -p data/db")
  info("$ mongod --dbpath data/db/  --logpath=mongod.log  --port 27017 --rest --fork")
  info("----------------------------------------")
  info("Stops mongodb using :")
  info("$ mongo --eval 'db.shutdownServer()' admin")
  info("----------------------------------------")

  def populateIfRequired() {
    val db = use("world")
    val people = db("people")
    val addresses = db("addresses")
    Await.ready(Future.sequence(List(people.drop(), addresses.drop())), 2.seconds)

    val finserts = List(
      people.insert(BD("name" -> "alphonse", "age" -> 30, "sex" -> "male")),
      people.insert(BD("name" -> "bob", "age" -> 20, "sex" -> "male")),
      people.insert(BD("name" -> "justine", "age" -> 30, "sex" -> "female")),
      people.insert(BD("name" -> "claire", "age" -> 8, "sex" -> "female")),
      people.insert(BD("name" -> "peter", "age" -> 12, "sex" -> "male")),
      people.insert(BD("name" -> "tom", "age" -> 5, "sex" -> "male")),
      people.insert(BD("name" -> "julie", "age" -> 24, "sex" -> "female")),
      people.insert(BD("name" -> "bob", "age" -> 18, "sex" -> "male")),
      people.insert(BD("name" -> "david", "age" -> 14, "sex" -> "male")),
      people.insert(BD("name" -> "lisa", "age" -> 22, "sex" -> "female")),
      people.insert(BD("name" -> "brice", "age" -> 60, "sex" -> "male")),
      people.insert(BD("name" -> "jule", "age" -> 90, "sex" -> "male")))
    Await.ready(Future.sequence(finserts), 2.seconds)
  }

  def isPrime(v: BigInt): Boolean = {
    @tailrec
    def checkUpTo(curr: BigInt, upTo: BigInt): Boolean =
      if (curr >= upTo) true
      else (v /% curr) match {
        case (_, mod) if mod == 0 => false
        case (nextUpTo, _) => checkUpTo(curr + 1, nextUpTo + 1)
      }
    checkUpTo(2, v / 2 + 1)
  }

  
  case class CheckedValue(
      value:Long,
      isPrime:Boolean,
      digitCount:Long,
      primePosition:Long
      )
  implicit object SomeClassHandler extends BSONDocumentReader[CheckedValue] with BSONDocumentWriter[CheckedValue] {
    def read(doc: BSONDocument) = {
      CheckedValue(
          value = doc.getAs[Long]("value").get,
          isPrime = doc.getAs[Boolean]("isPrime").get,
          digitCount = doc.getAs[Long]("digitCount").get,
          primePosition = doc.getAs[Long]("primePosition").get
          )
    }
    def write(cv: CheckedValue) = {
      BSONDocument(
          "value" -> cv.value,
          "isPrime" -> cv.isPrime,
          "digitCount" -> cv.digitCount,
          "primePosition" -> cv.primePosition
          )
    }
}
      
      
  
  def populateBigDataIfRequired() {
    import math._
    val db = use("bigs")
    val primes = db("primes")
    var primeCount=0L
    
    def insert(value: Long) {
      val prime = isPrime(value)
      if (prime) primeCount+=1L
      val fins = primes.insert(
        BD(
          "value" -> value,
          "isPrime" -> prime,
          "digitCount" -> (log(value)/log(10L)+1).toLong,
          "primePosition" -> (if(prime) primeCount else -1L)
          ))
      Await.ready(fins, 1.second)
    }
    info("Required index : db.primes.createIndex({value:1})")
    info("Required index : db.primes.createIndex({primePosition:1})")
    val fall = for {
      lastValueDoc <- primes.find(BD()).sort(BD("value"-> -1)).cursor[BD].headOption.map(_.getOrElse(BD()))
      lastPrimeDoc <- primes.find(BD()).sort(BD("primePosition"-> -1)).cursor[BD].headOption.map(_.getOrElse(BD()))
    } yield {
      val lastValue = lastValueDoc.get("value") match {
        case Some(BSONLong(l))=> l
        case _ => 0L
      }
      val lastPrimePosition = lastPrimeDoc.get("primePosition") match {
        case Some(BSONLong(l))=> l
        case _ => 0L
      }
      primeCount = lastPrimePosition
      val howmany=500000L
      info(s"Generating new primes, starting from $lastValue, checking $howmany next values (lastPrimePosition=$lastPrimePosition")
      (1L+lastValue to 1L+lastValue+howmany).foreach{v => insert(v)}
      'done
    }
    fall.onFailure{case x =>
      info(x.toString)
      fail("NOK - try create an index on value field of primes collection")
    }
    Await.ready(fall, 20.minutes)
    info("populated")
  }

  before {
    driver = new MongoDriver()
    use = driver.connection(List("localhost:27017"))
    populateIfRequired()
    //populateBigDataIfRequired()
  }

  after {
    driver.close
  }

  // --------------------------------------------------------------------------------
  test("primes") {
    val bigs=use("bigs")
    val checked=bigs("primes")
    val cursor = checked.find(BD()).cursor[CheckedValue]
    
    val fall = 
      cursor
        .collect[List]()
        .map(_.filter(_.isPrime))
        .map(_.size)
    
     val sz = Await.result(fall, 30.seconds)
     info(s"got $sz primes, but not in an optimized way, as everything all primes are loaded into memory")
     sz should be >(0)
  }
  // --------------------------------------------------------------------------------
  test("perf test") {
    val db = use("world")
    val collection = db("people")

    val fops = for { _ <- 1 to 30 } yield {
      val query = BD("age" -> BD("$lte" -> 15))

      val cursor = collection.find(query).cursor[BSONDocument]
      val fop = cursor.collect[List]()
      for {
        list <- fop
        doc <- list
      } {
        //info("SOME: " + pretty(doc))
      }
      fop
    }

    Await.ready(Future.sequence(fops), 10000.milliseconds)
  }

  // --------------------------------------------------------------------------------
  test("Simple test") {
    val db = use("world")
    val collection = db("people")

    // ----------------- REMOVE -----------------
    def fremove() = {
      collection.remove(BD("name" -> "alphonse"))
    }

    // ----------------- INSERT -----------------

    def finsert() = {
      val newEntry =
        BD(
          "name" -> "alphonse",
          "age" -> 10,
          "sex" -> "male")
      collection.insert(newEntry)
    }

    def fqryall() = { // ----------------- QUERY ALL -----------------
      val cursor = collection.find(BD()).cursor[BSONDocument]
      val fop = cursor.collect[List]()
      for {
        list <- fop
        doc <- list
      } {
        //println("ALL: " + pretty(doc))
      }
      fop
    }

    def fqrysome() = { // ----------------- QUERY SOME -----------------
      val query = BD("age" -> BD("$lte" -> 15))
      val cursor = collection.find(query).cursor[BSONDocument]
      val fop = cursor.collect[List]()
      for {
        list <- fop
        doc <- list
      } {
        //println("SOME: " + pretty(doc))
      }
      fop
    }

    val fops = Future.sequence(
      List(
        fremove().map(_ => finsert()),
        fqryall(),
        fqrysome()))
    Await.ready(fops, 10000.milliseconds)

  }

  // --------------------------------------------------------------------------------
  ignore("Second test") {
    val db = use("training")
    val collection = db("scores")

    val query =
      BD(
        "kind" -> "quiz",
        "score" -> BD("$gte" -> 90))

    val cursor =
      collection
        .find(query)
        .sort(BD("score" -> -1))
        .cursor[BSONDocument]

    val fop = cursor.collect[List](100)
    for {
      result <- fop
      doc <- result
    } {
      println("got: " + pretty(doc))
    }

    Await.ready(fop, 1000.milliseconds)
  }

}
