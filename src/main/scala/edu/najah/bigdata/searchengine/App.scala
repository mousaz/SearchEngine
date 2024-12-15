package edu.najah.bigdata.searchengine

import edu.najah.bigdata.searchengine.InvertedIndexManager.{generateIndex, readInvertedIndexFile}
import org.apache.log4j.varia.NullAppender
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase}
import org.mongodb.scala._
import org.mongodb.scala.bson.codecs.Macros
import org.mongodb.scala.model.Filters

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.control.Breaks.{break, breakable}

object App {
  case class TermDocLocation(docName: String, locations: Seq[Long])
  case class Term(text: String, freq: Int, locations: Seq[TermDocLocation])

  def setup(
             spark: SparkSession,
             termsCollection:
             MongoCollection[Term]): RDD[(String, Int, Array[(String, Array[Long])])] = {
    // Generate the inverted index
    val invertedIndexRdd = generateIndex(
      dataFilesFolderPath = "data/documents",
      indexFilePath = "data/inverted-index",
      spark)

    // Insert items to mongodb in chunks
    invertedIndexRdd
      .collect()
      .foreach({
        case (w, c, l) =>
          val future = termsCollection.insertOne(
            Term(w, c, l.map(i => TermDocLocation(i._1, i._2)))).toFuture()
          Await.result(future, 10.seconds)
      })

    invertedIndexRdd
  }

  def runQueryOnSpark(
                       invertedIndexRdd: RDD[(String, Int, Array[(String, Array[Long])])],
                       terms: Array[String]): (Array[String], Array[(String, Array[Long])]) = {

    val matchedTerms = invertedIndexRdd
      .map(term => (term._1, term._3))
      .filter {
        case (term, _) => terms.contains(term)
      }
      .collectAsMap()
      .toMap

    getMatchedPhrase(matchedTerms, terms)
  }

  def runQueryOnMongo(
                       termsCollection: MongoCollection[Term],
                       terms: Array[String]): (Array[String], Array[(String, Array[Long])]) = {

    val query = Filters.or(terms.map(term => Filters.eq("text", term)): _*)
    val future = termsCollection.find(query).toFuture()
    val results = Await.result(future, 10.seconds)
    val resultsAsMap = results.map(d => {
      (d.text, d.locations.map(l => (l.docName, l.locations.toArray)).toArray)
    }).toMap
    getMatchedPhrase(resultsAsMap, terms)
  }

  def getMatchedPhrase(
                        singleMatches: Map[String, Array[(String, Array[Long])]],
                        terms: Array[String]): (Array[String], Array[(String, Array[Long])]) = {

    var result: (Array[String], Array[(String, Array[Long])]) = (Array(), Array())
    terms.foreach(term => {
      if (singleMatches.contains(term)) {
        val docsForTerm = singleMatches(term)
        val (addedTerms, addedDocs) = result
        if (addedTerms.isEmpty && addedDocs.isEmpty) {
          result = (Array(term), docsForTerm)
        } else {
          var multiTermDocs: Array[(String, Array[Long])] = Array()
          addedDocs.foreach {
            case (docName, locations) => {
              val foundDocs = docsForTerm.filter(d => d._1.equals(docName))
              if (!foundDocs.isEmpty) {
                var foundLocations: Array[Long] = Array()
                locations.foreach(l => {
                  if (foundDocs(0)._2.contains(l + addedTerms.length)) {
                    foundLocations = foundLocations:+ l
                  }
                })
                if (!foundLocations.isEmpty) {
                  multiTermDocs = multiTermDocs:+ (docName, foundLocations)
                }
              }
            }
          }
          result = (addedTerms:+ term, multiTermDocs)
        }
      }
    })
    result
  }

  def main(args: Array[String]): Unit = {
    import org.apache.log4j._
    Logger.getRootLogger.setLevel(Level.ERROR)

    // code segment used to prevent excessive logging
    BasicConfigurator.configure(new NullAppender)

    val conf = new SparkConf()
      .setAppName("SearchEngine")
      .setMaster("local[*]")

    // Create the Spark Context
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val mongoClient: MongoClient = MongoClient(/*"mongodb://localhost:27017"*/)
    val codecRegistry = fromRegistries(
      fromProviders(
        Macros.createCodecProvider[Term](),
        Macros.createCodecProvider[TermDocLocation]()
      ),
      DEFAULT_CODEC_REGISTRY)
    val database: MongoDatabase = mongoClient.getDatabase("SearchEngine").withCodecRegistry(codecRegistry)
    val termsCollection: MongoCollection[Term] = database.getCollection("Terms")

    // One time setup
    val invertedIndexRdd = setup(spark, termsCollection)
    //    val invertedIndexRdd = readInvertedIndexFile("data/inverted-index", spark)

    breakable {
      while (true) {
        print("Enter search query [$ to exit]: ")
        val userQuery = scala.io.StdIn.readLine()
        if (userQuery.equals("$")) break
        val normalizedTerms = userQuery.split(" ").map(_.toLowerCase()).filter(_.length > 2)

        val sparkResult = withTime(runQueryOnSpark(invertedIndexRdd, normalizedTerms))
        val mongoResult = withTime(runQueryOnMongo(termsCollection, normalizedTerms))

        printResults(sparkResult, "Spark")
        printResults(mongoResult, "Mongo")
        println()
      }
    }
  }

  def withTime[T](action: => T): (T, Long) = {
    val start = System.nanoTime
    val result = action
    val end = System.nanoTime
    (result, (end - start) / 1000000)
  }

  def printResults(
                    result: ((Array[String], Array[(String, Array[Long])]), Long),
                    querySource: String): Unit = {
    val ((foundTerms, docLocations), timeTaken) = result
    val locationsStr = docLocations.map(l => s"${l._1}:[${l._2.mkString(", ")}]")
    println(s"Query ran on $querySource and toke $timeTaken ms")

    if (foundTerms.isEmpty) {
      println("Not found")
    } else {
      println(s"'${foundTerms.mkString(" ")}' found in:\n${locationsStr.mkString("\n")}")
    }

    println("--------------------------------------")
  }
}