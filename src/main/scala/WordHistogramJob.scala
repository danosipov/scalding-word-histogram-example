import com.twitter.scalding._

class WordHistogramJob(args: Args) extends Job(args) {
  val inputPath = args("input")
  val ignorePath = args("ignore")
  val outputPath = args("output")

  val toIgnore: Grouped[String, String] = TypedPipe.from(TextLine(ignorePath)).groupBy(identity)

  val words: TypedPipe[(String, Int)] = TypedPipe.from(TextLine(inputPath))
    .flatMap(tokenize)
    .groupBy(identity)
    .leftJoin(toIgnore)
    .mapValues { // TODO: Use flatMapValues
      case (word, None) => Some((word, 1)) // Filter out frequently occuring words
      case _ => None
    }
    .values
    .flatMap(v => v) // TODO remove once flatMapValues is in Scalding

  val totalPipe = words.map(_._2).sum

  words
    .groupBy(_._1)
    .size
    .toTypedPipe
    .mapWithValue(totalPipe) {
      case ((word, count), Some(total)) => (word, count.toDouble / total)
    }
    .groupAll
    .sortedReverseTake(10)(Ordering.by(_._2))
    .flattenValues
    .values
    .write(TypedTsv(outputPath))

  def tokenize(line: String) = {
    line.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
