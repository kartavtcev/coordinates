package example

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import monix.execution.CancelableFuture

import scala.concurrent.Await
import scala.concurrent.duration._
import monix.reactive.Observable




object Hello extends App {
  implicit val ctx = monix.execution.Scheduler.Implicits.global

  val id1 = "655f7545"
  val id2 = "78b85537"

  val reader = new BufferedReader(new InputStreamReader(
    new FileInputStream("reduced.csv"), "UTF-8"))

  // Creating new DateTime is expensive, just work with strings . split
  // Skip precision in time & coordinates
  lazy val cf : CancelableFuture[Unit] =
    Observable.fromLinesReader(reader)
      .drop(1)
      .filter(l => l.endsWith(id1) || l.endsWith(id2))
      .map{ ln =>
        val l = ln.split(",")
        val t = (l(0).split("T|\\.")(1)).split(":")
        ((t(0), t(1), t(2)), l(1).split("\\.")(0).toInt, l(2).split("\\.")(0).toInt, l(3).toInt, l(4))
      }
      .foreach{   println(_)    }

  Await.result(cf, Duration.Inf)

}