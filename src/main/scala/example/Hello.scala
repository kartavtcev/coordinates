package example

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import monix.execution.CancelableFuture
import monix.reactive.Observable

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success


object Hello extends App {
  implicit val ctx = monix.execution.Scheduler.Implicits.global

  val id1 = (1, "655f7545")
  val id2 = (2, "78b85537")
  val parser = new Parser(id1, id2)

  val reader = new BufferedReader(new InputStreamReader(
    new FileInputStream("reduced.csv"), "UTF-8"))

  lazy val cf : CancelableFuture[Unit] =
    Observable.fromLinesReader(reader)
      .drop(1)
      .filter(parser.filterPred)
      .map(parser.parse)
      .collect { case Success(t) => t }
      //.groupBy{ case (_,_,_,floor,id) => (floor, id) }
      .foreach{   println    }

  Await.result(cf, Duration.Inf)

}