package utils

import java.io.{BufferedReader, File, FileNotFoundException, InputStreamReader}
import java.util.stream.Collectors

import org.apache.commons.text.StringSubstitutor
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats

import scala.io.Source
import scala.collection.JavaConverters._

object FileUtils {
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else if (d.isFile) {
      List(d)
    } else {
      throw new FileNotFoundException(s"No Files to Run ${dir}")
    }
  }

  def getContentFromFileAsString(file: File): String = {
    scala.io.Source.fromFile(file).mkString //    //By scala.io. on read spark fail with legit error when path does not exists
  }

  def readConfigurationFile(path: String): String = {
    val fileContents = Source.fromFile(path).getLines.mkString("\n")
    val interpolationMap = System.getProperties().asScala ++= System.getenv().asScala
    StringSubstitutor.replace(fileContents, interpolationMap.asJava)
  }

  def readFileWithHadoop(path: String, sparkSession: SparkSession): String = {
    val file = new Path(path)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val fs = file.getFileSystem(hadoopConf)
    val fsFile = fs.open(file)
    val reader = new BufferedReader(new InputStreamReader(fsFile))
    reader.lines.collect(Collectors.joining)
  }
}