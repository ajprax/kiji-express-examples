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

import com.twitter.scalding.Args

import org.kiji.express._
import org.kiji.express.flow._

/**
 * Counts the words in each posting in the newsgroup table.
 *
 * Usage:
 *   express hdfs <path/to/this/jar> \
 *       org.kiji.express.examples.NewsgroupPostCounter \
 *       --table kiji://.env/default/postings
 */
class NewsgroupPostCounter(args: Args) extends KijiJob(args) {
  val tableUri: String = args("table")

  KijiInput(tableUri, "info:post" -> 'post)
      // Count the words in each post.
      .map('post -> 'postLength) { cells: Iterable[Cell[CharSequence]] =>
        // Get the posting text.
        val text = cells.head.datum

        // Regular expression for matching words. For more information see:
        // http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html.
        val wordRegex = """\b\p{L}+\b""".r

        // Split the text up into words.
        wordRegex
            .findAllIn(text)
            .length
      }
      // Write the length of each post to the specified table.
      .write(KijiOutput(tableUri, 'postLength -> "info:postLength"))
}
