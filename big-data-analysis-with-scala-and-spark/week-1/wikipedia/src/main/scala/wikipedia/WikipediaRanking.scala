package wikipedia

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

// See: https://www.coursera.org/learn/scala-spark-big-data/programming/QcWcs/wikipedia
object WikipediaRanking {
  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf()
    .setAppName("WikipediaRanking")
    .setMaster("local[*]")

  val sc: SparkContext = new SparkContext(conf)

  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(WikipediaData.parse)
  wikiRdd.cache()

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    val accumulateOp = (total: Int, article: WikipediaArticle) => {
      if (article.mentionsLanguage(lang)) {
        total + 1
      } else {
        total
      }
    }
    val combineOp = (count1: Int, count2: Int) => count1 + count2
    rdd.filter(_.mentionsLanguage(lang)).aggregate(0)(accumulateOp, combineOp)
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs
      .map((lang: String) => {
        (lang, occurrencesOfLang(lang, rdd))
      })
      .sortWith(_._2 > _._2)
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    rdd
      .flatMap((article: WikipediaArticle) => {
        langs
          .filter((lang: String) => {
            article.mentionsLanguage(lang)
          })
          .map((lang: String) => {
            (lang, article)
          })
      })
      .groupByKey()
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index
      .map({
        case (lang, articles) => {
          (lang, articles.size)
        }
      })
      .sortBy(-_._2)
      .collect()
      .toList
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd
      .flatMap((article: WikipediaArticle) => {
        langs
          .filter((lang: String) => {
            article.mentionsLanguage(lang)
          })
          .map((lang: String) => {
            (lang, 1)
          })
      })
      .reduceByKey((count1: Int, count2: Int) => {
        count1 + count2
      })
      .collect()
      .toList
      .sortWith(_._2 > _._2)
  }

  def main(args: Array[String]) {
    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)

    // Processing Part 1: naive ranking took 16753 ms.
    // Processing Part 2: ranking using inverted index took 13348 ms.
    // Processing Part 3: ranking using reduceByKey took 7274 ms.

    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
