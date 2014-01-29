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

import fr.janalyse.ssh._

import rx.lang.scala._
import rx.lang.scala.subjects._

@RunWith(classOf[JUnitRunner])
class SSHObservablesTest extends FunSuite with ShouldMatchers {

  test("Simple test") {
    Dummy.message should startWith("Hello")
    info("Test done")
  }

  def rxFromSSH(cmd:String, opts:SSHOptions):Observable[String] = {
    val ssh = SSH(opts)
    
    val subject = Subject[String]()
    def resultOnNext(result: ExecResult) {
      result match {
        case e: ExecPart => subject.onNext(e.content)
        case e: ExecEnd => subject.onCompleted ; ssh.close
        case ExecTimeout => subject.onError(new Exception("Timeout"))
      }
    }
    ssh.run(cmd, resultOnNext)
    subject
  }
  
  test("ssh to observables") {
    val timeRE = """.*(\d\d:\d\d:\d\d).*""".r
    val opts  = SSHOptions("localhost", username="test")
    
    // Create observables...
    val date  = rxFromSSH("while [ 1 ] ; do date ; sleep 1 ; done", opts)
    val time  = date.map{case timeRE(time)=>time}
    val secs  = time.map(_.split(":")(2).toInt)
    
    // And now let's subscribe...
    date.subscribe(s => println("date="+s))
    time.subscribe(s => println("time="+s))
    secs.subscribe(s => println("secs="+s))
    
    println("******* MidStep & wait 5s *******")
    Thread.sleep(5*1000L)
    
    val pairsSecs = secs.filter(_ % 2 == 0).map(_*2)
    pairsSecs.subscribe(s => println("PROCESSED="+s))
    
    
    println("******* End reached & wait 10s *******")    
    Thread.sleep(10*1000L)
  }
}
