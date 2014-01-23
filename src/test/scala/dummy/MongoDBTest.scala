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

@RunWith(classOf[JUnitRunner])
class MongoDBTest extends FunSuite with ShouldMatchers with BeforeAndAfterEach {

  var driver: MongoDriver = _
  var use: MongoConnection = _

  override def beforeEach() {
    driver = new MongoDriver
    use = driver.connection(
      List(
        "localhost:27017",
        "localhost:27018",
        "localhost:27019"))
  }

  override def afterEach() {
    //connection.close => generates akka errors message that goes to the deadletter
    driver.close
  }

  // --------------------------------------------------------------------------------
  ignore("perf test") {
    val db = use("orange")
    val collection = db("people")

    val fops = for { _ <- 1 to 30 } yield {
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

    Await.ready(Future.sequence(fops), 10000.milliseconds)
  }

  // --------------------------------------------------------------------------------
  test("Simple test") {
    val db = use("orange")
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
        println("ALL: " + pretty(doc))
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
        println("SOME: " + pretty(doc))
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
  ignore("query statistics") {
    val adb = use("admin")
    val fop = adb.command(Status)
    for {
      status <- fop
      (key, value) <- status
    } {
      value match {
        case e: BSONDocument => println(s"STATUS : $key = ${pretty(e)}")
        case _ => println(s"STATUS : $key = ${value}")
      }
    }

    Await.ready(fop, 1000.milliseconds)
  }

  // --------------------------------------------------------------------------------
  test("Second test") {
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
      println("got: " + BD.pretty(doc))
    }

    Await.ready(fop, 1000.milliseconds)
  }

}
