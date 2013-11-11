package org.kiji.express.examples

import scala.util.Random

import com.twitter.scalding.Args
import org.apache.avro.Schema

import org.kiji.express.flow.{QualifiedColumnRequestOutput, KijiInput, KijiJob, KijiOutput}

class NewsgroupSegmenter(args: Args) extends KijiJob(args) {
  val tableURIString: String = args("table")
  val ratio: Int = args.getOrElse("trainToTestRatio", "10").toInt

  KijiInput(tableURIString, "info:group" -> 'group)
      .map(() -> 'segment) {
        _: Unit => if (Random.nextInt(ratio) >= 1) 1 else 0
      }
      .write(KijiOutput(tableURIString, Map('segment ->
          QualifiedColumnRequestOutput("info:segment", Schema.create(Schema.Type.INT)))))
}
