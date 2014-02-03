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
import fr.janalyse.primes.PrimesGenerator
import fr.janalyse.primes.CheckedValue

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

  implicit object CheckedValueHandler
    extends BSONDocumentReader[CheckedValue[Long]]
    with BSONDocumentWriter[CheckedValue[Long]] {

    def read(doc: BSONDocument) = {
      CheckedValue(
        value = doc.getAs[Long]("value").get,
        isPrime = doc.getAs[Boolean]("isPrime").get,
        digitCount = doc.getAs[Long]("digitCount").get,
        nth = doc.getAs[Long]("nth").get)
    }
    def write(cv: CheckedValue[Long]) = {
      BSONDocument(
        "value" -> cv.value,
        "isPrime" -> cv.isPrime,
        "digitCount" -> cv.digitCount,
        "nth" -> cv.nth)
    }
  }

  def populatePrimesIfRequired(howmany:Int=10000) {
    import math._
    val db = use("primes")
    val primes = db("values")
    val lastPrime = db("lastPrime")
    val lastNotPrime = db("lastNotPrime")

    def insert(checked: CheckedValue[Long]) {
      val impacted = if (checked.isPrime) lastPrime else lastNotPrime
      val f1 = primes.insert(checked)
      val f2 = for {
        _ <- impacted.remove(BD())
        _ <- impacted.insert(checked)
      } yield 'done

      Await.ready(Future.sequence(List(f1, f2)), 5.second)
    }

    info("Required index : db.values.createIndex({value:1})")
    info("Required index : db.values.createIndex({isPrime:1, nth:1})")

    val fall = for {
      foundLastPrime <- lastPrime.find(BD()).cursor[CheckedValue[Long]].headOption
      foundLastNotPrime <- lastNotPrime.find(BD()).cursor[CheckedValue[Long]].headOption
    } yield {
      val pgen = new PrimesGenerator[Long]
      
      val foundLast = for {
        flp <- foundLastPrime
        flnp <- foundLastNotPrime
      } yield if (flp.value > flnp.value) flp else flnp
          
      val primeNth = foundLastPrime.map(_.nth).getOrElse(1L)
      val notPrimeNth = foundLastNotPrime.map(_.nth).getOrElse(0L)
      val resuming = foundLast.isDefined
      
      val resumedStream = pgen.checkedValues(foundLast.getOrElse(CheckedValue.first), primeNth, notPrimeNth) match {
        case s if resuming => s.tail
        case s => s
      }
      
      info(s"Generating new primes, starting from ${resumedStream.head}, checking $howmany next values ")
      for { checkedValue <- resumedStream.take(howmany) } { insert(checkedValue) }
      'done
    }
    fall.onFailure {
      case x =>
        info(x.toString)
        fail("NOK - try create an index on value field of primes collection")
    }
    Await.ready(fall, 30.minutes)
    info("populated")
  }

  before {
    driver = new MongoDriver()
    use = driver.connection(List("localhost:27017"))
    populateIfRequired()
    populatePrimesIfRequired()
  }

  after {
    driver.close
  }

  // --------------------------------------------------------------------------------
  test("primes") {
    val db = use("primes")
    val checked = db("values")
    val cursor = checked.find(BD()).cursor[CheckedValue[Long]]

    val fall =
      cursor
        .collect[List]()
        .map(_.filter(_.isPrime))
        .map(_.size)

    val sz = Await.result(fall, 30.seconds)
    info(s"got $sz primes, but not in an optimized way, as everything all primes are loaded into memory")
    sz should be > (0)
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
