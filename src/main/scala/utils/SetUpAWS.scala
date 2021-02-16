package utils

object SetUpAWS {
  def setupAwsCreds() = {
    import scala.io.Source

    for (line <- Source.fromFile("config/aws_creds.txt.dev").getLines) {
      val fields = line.split(" ")
      //println (line)
      println(fields(0) + "=" + fields(1))
      if (fields.length == 2) {

        System.setProperty(fields(0), fields(1))
      }
    }
  }

}