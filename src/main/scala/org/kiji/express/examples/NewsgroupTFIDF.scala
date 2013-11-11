package org.kiji.express.examples

import com.twitter.scalding.{Tsv, Args}

import org.kiji.express.flow.KijiInput
import org.kiji.express.flow.KijiJob
import org.kiji.express.{EntityId, Cell}

/**
 * Calculates the number of posts which contain each word regardless of category.
 *
 * @param args to the job. These get parsed in from the command line by Scalding.  Within your own
 *             KijiJob, `args("input")` will evaluate to "SomeFile.txt" if your command line contained the
 *             argument `--input SomeFile.txt`
 */
class NewsgroupDF(args: Args) extends KijiJob(args) {
  val tableURIString: String = args("table")
  val outFile: String = args("out-file")

  KijiInput(tableURIString, "info:segment" -> 'segment, "info:post" -> 'postText)
    .filter('segment) {
      segment: Iterable[Cell[Int]] => segment.head.datum == 1
    }
    .flatMapTo('postText -> 'word) {
      postText: Iterable[Cell[CharSequence]] => NewsgroupTFIDF.uniquelyTokenize(postText.head.datum)
    }
    .groupBy('word) {
      _.size('documentFrequency)
    }
    .write(Tsv(outFile))
}

/**
 * Calculates the number of posts within each category which contain each word.
 *
 * @param args to the job. These get parsed in from the command line by Scalding.  Within your own
 *             KijiJob, `args("input")` will evaluate to "SomeFile.txt" if your command line contained the
 *             argument `--input SomeFile.txt`
 */
class NewsgroupTF(args: Args) extends KijiJob(args) {
  val tableURIString: String = args("table")
  val outFile: String = args("out-file")

  KijiInput(tableURIString, "info:segment" -> 'segment, "info:post" -> 'postText)
    .filter('segment) {
      segment: Iterable[Cell[Int]] => segment.head.datum == 1
    }
    .map('entityId -> 'group) { entityId: EntityId => entityId(0) }
    .flatMap('postText -> 'word) {
      postText: Iterable[Cell[CharSequence]] => NewsgroupTFIDF.uniquelyTokenize(postText.head.datum)
    }
    .groupBy('group, 'word) { _.size('termFrequency) }
    .write(Tsv(outFile))
}

//class NewsgroupTFIDF(args: Args) extends KijiJob(args) {
//  val tableURIString: String = args("table")
//  val outFile: String = args("out-file")
//
//
//}

object NewsgroupTFIDF {
  def tokenize(post: CharSequence): Iterable[String] = {
    // Regular expression for matching words. For more information see:
    // http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html.
    val wordRegex = """\b\p{L}+\b""".r

    // Split the text up into words.
    wordRegex
        .findAllIn(post)
        .map { word: String => word.toLowerCase }
        .toIterable
  }

  def uniquelyTokenize(post: CharSequence): Set[String] = {
    tokenize(post).toSet
  }

  def tf(post: CharSequence): Map[CharSequence, Int] = {
    tokenize(post).foldLeft(Map[CharSequence, Int]()) {
        (acc: Map[CharSequence, Int], token: CharSequence) =>
            acc.updated(token, acc.getOrElse(token, 0) + 1)
    }
  }
}
