package edu.najah.bigdata.searchengine

import java.nio.file.{FileSystems, Files, Path}
import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object InvertedIndexManager {
  /**
   * Generates inverted index RDD from the documents within
   * the provided data folder and saves it to the specified file.
   * @param dataFilesFolderPath Data folder path
   * @param indexFilePath path for the inverted index
   * @param spark SparkSession instance
   * @return RDD representation of the generated inverted index
   */
  def generateIndex(
                     dataFilesFolderPath: String,
                     indexFilePath: String,
                     spark: SparkSession): RDD[(String, Int, Array[(String, Array[Long])])] = {
    val dir = FileSystems.getDefault.getPath(dataFilesFolderPath)
    val files = Files.list(dir).iterator().asScala.filter(Files.isRegularFile(_))
    val invertedIndexFileRdd = files
      .toArray
      .sortWith(_.getFileName.toString < _.getFileName.toString)
      .map(file => {
        val fileName = file.getFileName.toString
        val df = spark.sparkContext.textFile(file.toString)
        df
          .flatMap(_.split(" ")) // Get Words
          .filter(_.length > 2)
          .zipWithIndex()
          .map {
            case (word, index) => (word.toLowerCase, Array(index + 1))
          }
          .reduceByKey(_ ++ _)
          .map {
            case (w, l) => (w, (1, Array((fileName, l))))
          }
      })
      .reduce((df1, df2) => df1.union(df2))
      .reduceByKey {
        case ((c1, l1), (c2, l2)) => (c1 + c2, l1 ++ l2)
      }
      .map {
        case (word, (count, locations)) => (word, count, locations)
      }
      .sortBy(_._1)

//    deleteDirectory(FileSystems.getDefault.getPath(indexFilePath))

    // word,5,d1>1#2#3;d2>5#77
    invertedIndexFileRdd
      .map {
        case (word, count, locations) => {
          val locationsStr = locations
            .map(loc => s"${loc._1}>${loc._2.mkString("#")}")
            .mkString(";")
          s"$word,$count,$locationsStr"
        }
      }
//      .saveAsTextFile(indexFilePath)

    invertedIndexFileRdd
  }

  /**
   * Reads the inverted index file and parses it to the expected format
   * @param filePath the path of the inverted index file
   * @param spark spark session instance
   * @return RDD[(String, Int, Array[(String, Array[Long])])]
   */
  def readInvertedIndexFile(
                             filePath: String,
                             spark: SparkSession): RDD[(String, Int, Array[(String, Array[Long])])] = {
    val rdd = spark.sparkContext.textFile(filePath)

    // word,5,d1>1#2#3;d2>5#77
    rdd.map {
      line => {
        val parts = line.split(",")
        parts match {
          case Array(term, freq, docLocations) =>
            val docLocationsObj = docLocations.split(";").map {
              doc: String => {
                val docParts = doc.split(">")
                docParts match {
                  case Array(docName, locations) =>
                    (docName, locations.split("#").map(_.toLong))
                }
              }
            }
            (term, freq.toInt, docLocationsObj)
        }
      }
    }
  }

  private def deleteDirectory(filePath: Path): Unit = {
    if (Files.notExists(filePath))
      return

    if (Files.isDirectory(filePath)) {
      println(s"Deleting files in directory ${filePath.toString}")
      Files.list(filePath).forEach(deleteDirectory)
    }

    println(s"Deleting file ${filePath.toString}")
    Files.delete(filePath)
  }
}
