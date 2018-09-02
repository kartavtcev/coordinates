package example

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import com.typesafe.scalalogging.StrictLogging
import monix.execution.CancelableFuture
import monix.reactive.Observable

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}


object MeetupApp extends StrictLogging with App {

  val t0 = System.nanoTime()

  implicit val ctx = monix.execution.Scheduler.Implicits.global

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
      .collect{ case Some(t) => t }
      // 8458 records for 2 IDs: 655f7545, 78b85537
      // more often: replace 655f7545 with 5e7b40e1    ; two all different coordinates: 600dfbe2    & 3c3649fb /    285d22e4 / 74d917a1 /
      // NOT FOUND INTERSECTION: 285d22e4 / 74d917a1
      // p.s. try 600dfbe2 & 5e7b40e1
      .consumeWith(ProcessorConsumer.value)
      .foreach{
        case  Right(list) => list.foreach(println)
        case Left(msg) => println(msg)
      }

  Await.result(cf, Duration.Inf)

  val t1 = System.nanoTime()
  println(s"Elapsed run-time: ${(t1-t0)/1000000} ms")
}