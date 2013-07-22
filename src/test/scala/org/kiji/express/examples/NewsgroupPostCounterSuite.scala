/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.examples

import scala.collection.mutable.Buffer

import com.twitter.scalding.JobTest

import org.kiji.express._
import org.kiji.express.flow.DSL._
import org.kiji.express.util.Resources._
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

class NewsgroupPostCounterSuite extends KijiSuite {
  // Set up Kiji table for input.
  val layout: KijiTableLayout = {
    KijiTableLayouts.getTableLayout("org/kiji/express/examples/layout/postings.json")
  }

  val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
    table.getURI().toString()
  }

  val testInput: List[(EntityId, KijiSlice[String])] = List(
      ( EntityId(uri)("row01"), slice("info:post", (0L, "hello hello hello     hello")) ),
      ( EntityId(uri)("row02"), slice("info:post", (0L, "hello    \nworld")) ),
      ( EntityId(uri)("row03"), slice("info:post", (0L, "world")) ),
      ( EntityId(uri)("row04"), slice("info:post", (0L, "hello")) ))


  // A function to validate the test output.
  def validateTest(outputBuffer: Buffer[(EntityId, KijiSlice[Int])]) {
    assert(4 === outputBuffer.size)

    // Validate that the output is as expected.
    val outputMap = outputBuffer.toMap
    assert(4 === outputMap(EntityId(uri)("row01")).getFirstValue())
    assert(2 === outputMap(EntityId(uri)("row02")).getFirstValue())
    assert(1 === outputMap(EntityId(uri)("row03")).getFirstValue())
    assert(1 === outputMap(EntityId(uri)("row04")).getFirstValue())
  }

  test("NewsgroupPostCounter counts words using scalding's local mode.") {
    JobTest(new NewsgroupPostCounter(_))
        .arg("table", uri)
        .source(KijiInput(uri)("info:post" -> 'post), testInput)
        .sink(KijiOutput(uri)('postLength -> "info:postLength"))(validateTest)
        .run
        .finish
  }

  test("NewsgroupPostCounter counts words using hadoop.") {
    JobTest(new NewsgroupPostCounter(_))
        .arg("table", uri)
        .source(KijiInput(uri)("info:post" -> 'post), testInput)
        .sink(KijiOutput(uri)('postLength -> "info:postLength"))(validateTest)
        .runHadoop
        .finish
  }
}
