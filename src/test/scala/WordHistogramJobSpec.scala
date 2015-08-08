import com.twitter.scalding._
import org.scalatest.{Matchers, FunSuite}

import scala.io.Source

class WordHistogramJobSpec  extends FunSuite with Matchers with FieldConversions {
  val input = getSource("src/test/resources/zeppelin")
  val ignore = getSource("src/test/resources/ignore")

  JobTest(manifest[WordHistogramJob])
    .arg("input", "inputFile")
    .arg("ignore", "ignoreFile")
    .arg("output", "outputFile")
    .source(TextLine("inputFile"), input)
    .source(TextLine("ignoreFile"), ignore)
    .sink[(String, Double)](TypedTsv[(String, Double)]("outputFile")) { outputBuffer =>
      outputBuffer.foreach(println)
    }
    .run
    .finish

  def getSource(file: String) = {
    Source.fromFile(file).getLines()
      .toList.map(line => (line.hashCode, line))
  }
}
