package example

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import com.typesafe.scalalogging.StrictLogging
import monix.execution.CancelableFuture
import monix.reactive.Observable

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}


object Hello extends StrictLogging with App {

  implicit val ctx = monix.execution.Scheduler.Implicits.global

  def avg(xs: Iterable[Int]) = xs.sum / xs.count(_=>true)

  val id1 = (1, "655f7545")
  val id2 = (2, "78b85537")
  val parser = new Parser(id1, id2)
  val fileName = "reduced.csv"

  val reader = new BufferedReader(new InputStreamReader(
    new FileInputStream(fileName), "UTF-8"))

  lazy val cf : CancelableFuture[Unit] =
    Observable.fromLinesReader(reader)
      .drop(1)
      .filter(parser.filterPred)
      .map(parser.parse)
      .collect {
        case Success(t) => Some(t)
        case Failure(e) =>
          logger.error(e.toString)
          None
      }
      .collect{ case Some(t) => t } // 8458 records for 2 IDs: 655f7545, 78b85537
      .consumeWith(Processor.aggregateConsumer)
      // .runAsync
      .foreach{ list => list.foreach(println) }

  println("all finished ???")

  Await.result(cf, Duration.Inf)

  println("all finished.")

}