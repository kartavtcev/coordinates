package example

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import com.typesafe.scalalogging.StrictLogging
import monix.execution.CancelableFuture
import monix.reactive.Observable

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}


object MeetupApp extends StrictLogging {

  def main(args: Array[String]) = {

    if(args.length != 2 || args(0) == args(1)) {    // https://stackoverflow.com/questions/2066307/how-do-you-input-commandline-argument-in-intellij-idea
      println("Input 2 different ids.")
    } else {
      val t0 = System.nanoTime()

      implicit val ctx = monix.execution.Scheduler.Implicits.global

      val id1 = (1, args(0))
      val id2 = (2, args(1))
      val parser = new Parser(id1, id2)
      val fileName = "reduced.csv"

      val reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(fileName), "UTF-8"))

      lazy val cf: CancelableFuture[Unit] =
        Observable.fromLinesReader(reader)
          .drop(1)
          .filter(parser.filterPred)
          .map(parser.parse)
          .collect {
            case Success(t) => Some(t)
            case Failure(e) =>
              logger.error(e.toString)    // Effect
              None
          }
          .collect { case Some(t) => t }
          .consumeWith(ProcessorConsumer.value)
          .foreach {
            case Right(list) => list.foreach(println)
            case Left(msg) => println(msg)
          }

      Await.result(cf, Duration.Inf)

      val t1 = System.nanoTime()
      println(s"Elapsed run-time: ${(t1 - t0) / 1000000} ms")
    }
  }
}