package DevClass

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// user defined class 
import utils.FileUtils

object LoadUtilsDemo {

  def main(args: Array[String]): Unit = {

    println(">>> import user defined class")

    val file_list = FileUtils.getListOfFiles("data")

    println(file_list)

    //FileUtils.getContentFromFileAsString(_)

    //FileUtils.readFileWithHadoop(_,_)

  }

}