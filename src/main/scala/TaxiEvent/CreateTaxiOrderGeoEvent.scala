package TaxiEvent

import java.io.PrintWriter
import java.net.ServerSocket
import java.util.Random

object CreateTaxiOrderGeoEvent {

  def main(args: Array[String]) {

    val port = 44444
    val viewsPerSecond = 10
    val sleepDelayMs = (1000.0 / viewsPerSecond).toInt
    val listener = new ServerSocket(port)
    println(s"Listening on port: $port")

    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println(s"Got client connected from: ${socket.getInetAddress}")
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {

            val topic = "payment"
            val r = scala.util.Random
            val id = r.nextInt(10000000)
            val tour_value = r.nextDouble() * 100
            val id_driver = r.nextInt(10)
            val id_passenger = r.nextInt(100)
            val event_date = System.currentTimeMillis
            val lon = 40.71 + (r.nextFloat - 0.5) * 0.001 // generate random long 
            var lat = 74.00 + (r.nextFloat - 0.5) * 0.001 // generate random long 

            // val payload =
            //   s"""
            //      |{ "id": $id,
            //      |  "event_date": $event_date,
            //      |  "tour_value": $tour_value,
            //      |  "id_driver": $id_driver,
            //      |  "id_passenger": $id_passenger,
            //      |  "long" : $long,
            //      |  "lat" : $lat}
            //   """.stripMargin


            val payload =
              s"""
                { "id": $id, "event_date": $event_date, "tour_value": $tour_value, "id_driver": $id_driver, "id_passenger": $id_passenger, "location" : [$lat, $lon]}
                """.stripMargin

            Thread.sleep(sleepDelayMs)
            out.write(payload)
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}