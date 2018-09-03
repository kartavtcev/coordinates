package example

import org.scalatest._

import scala.concurrent.duration.Duration

class ProcessorSpec extends FlatSpec with Matchers {
  "Processor " should "run findMeetups on next hour record, and fill meetups collection." in {

    val id1 = (1, "600dfbe2")
    val id2 = (2, "5e7b40e1")

    val r1Current = Record(id1._1, (19, (16, 0, 6)), (113, 101, 1))
    val r2 = Record(id2._1, (19, (16, 0, 6)), (110, 100, 1))

    val r1Next = Record(id1._1, (19, (17, 10, 6)), (110, 100, 1))
    val r2Next = Record(id2._1, (19, (17, 15, 6)), (100, 80, 1))



    implicit val ctx = monix.execution.Scheduler.Implicits.global
    val processor = Processor.create.runSyncUnsafe(Duration.Inf)

    processor.onNext(r1Current)
    processor.onNext(r2)
    processor.meetups.read.runSyncUnsafe(Duration.Inf) shouldBe List()

    processor.onNext(r1Next)
    processor.onNext(r2Next)
    processor.meetups.read.runSyncUnsafe(Duration.Inf) shouldBe List(Meet((Hour(16), Min(0)), Coordinate(111, 100, 1)))

  }
}


