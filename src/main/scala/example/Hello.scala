package example

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import scala.concurrent.Await
import scala.concurrent.duration._

import monix.reactive.Observable




object Hello extends App {
  implicit val ctx = monix.execution.Scheduler.Implicits.global

  /*
  val path = java.nio.file.Paths.get("").toAbsolutePath
  val from = java.nio.file.Paths.get(path + """\test.txt""")
  println(from)

  ///java.nio.file.Files.readAllLines(from, StandardCharsets.UTF_8)
    ///.forEach(println(_))

  readAsync(from, 30)
    .pipeThrough(utf8Decode) // decode utf8, If you need Array[Byte] just skip the decoding
    .foreach(println(_))
    // print each char
*/

  val reader = new BufferedReader(new InputStreamReader(
    new FileInputStream("reduced.csv"), "UTF-8"))

  //  Await.result(Observable.fromLinesReader(reader).foreach(println), scala.concurrent.duration.Duration.Inf)
  // FIXED !!! issue was a timeout !!!
  lazy val cf = Observable.fromLinesReader(reader)
    .drop(1)
    .foreach{ ln =>
    val l = ln.split(",")
    println(s"${l(0).split("T")(1)} ${l(1)} ${l(2)}")
  }
  Await.result(cf, Duration.Inf)

    //.transform(mapAsyncOrdered(nonIOParallelism)(println))//.foreach(println)

  //println(Observable.fromLinesReader(reader).countL.runSyncUnsafe(1 hour))
    //.foreach(println(_))

    //.drop(linesToSkip)
    //.transform(mapAsyncOrdered(nonIOParallelism)(parseLine))


}

/*
trait Greeting {
  lazy val greeting: String = "hello"
}
*/
