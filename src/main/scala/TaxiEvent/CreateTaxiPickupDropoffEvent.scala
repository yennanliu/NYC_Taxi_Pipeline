package TaxiEvent

import java.io.PrintWriter
import java.net.ServerSocket
import java.util.Random

object CreateTaxiPickupDropoffEvent {

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

            val r = scala.util.Random

            val topic = "drop_off_event"
            val rideId = r.nextInt(100000)
            val time = System.currentTimeMillis
            val isStart = "START"
            val lon = 40.71 + (r.nextFloat - 0.5) * 0.001 // generate random long 
            var lat = -74.00 + (r.nextFloat - 0.5) * 0.001 // generate random long 
            val passengerCnt = r.nextInt(10)
            val travelDist = r.nextFloat

            /* Expected event form : 
             * rideId: Long // unique id for each ride
             * time: DateTime // timestamp of the start/end event
             * isStart: Boolean // true = ride start, false = ride end
             * location: GeoPoint // lon/lat of pick-up/drop-off location
             * passengerCnt: short // number of passengers
             * travelDist: float // total travel distance, -1 on start events
             *
             * 6928296,2013-01-01 00:00:34,START,-73.982962999999998,40.742156000000001,1,-1
             * 6877650,2013-01-01 00:00:36,END,-73.991641999999999,40.726658999999998,1,0.10000000000000001
             */

            val payload = s"""$rideId,$time,$isStart,$lat,$lon,$passengerCnt,$travelDist \n""".stripMargin

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