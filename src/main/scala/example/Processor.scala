package example

import monix.eval.{Callback, MVar, Task}
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber

//class Processor(val test:  MVar[Int]) {    // return FIRST coords intersection on data   "your code should indicate if a meeting has occurred"

  // first coords

  // last coord from PREV periodv
  //val startDate: MVar[(Int, (Int, Int))] = MVar.empty
  //val startHourDataPerMin: MVar[Array[(Int, (Int, Int, Int))]] = MVar.empty   // array is per-minute ! 0-59 ...
  // count of Hours
  //val lastCoordFromPrevHour: (Int, (Int, Int, Int))

  //val test:  Task[MVar[Int]] //= MVar.empty

  // TODO: do 1 min shift for old data in a stream ... while consuming a new hour.
  // TODO: THEN PROCESS AN OLD DATA & MOVE TO NEW ONE. CURRENT = NEW ...
  // next hour
  //

  // TODO: second coords

  // PROCESS PARALLEL HOURDATA

class Processor(val test:  Task[MVar[Int]]) {
}

object Processor {

  val aggregateConsumer =
    new Consumer[Record, Any] {

      def createSubscriber(cb: Callback[Any], s: Scheduler) = {
        val out = new Subscriber.Sync[Record] {
          implicit val scheduler = s
          //private var sum = 0L
          val processor = MVar(0).map(new Processor(_))

          def onNext(elem: Record) = {
            processor.map(p => p.test.take.map( (v : Int) => p.test.put(v + 1)))
            /*
            for {
              v <- p.test.take
              _ <- p.test.put(v + 1)
            }
            */
            Continue
          }

          def onComplete(): Unit = {
            processor.map( t => t.test.take.map(v => cb.onSuccess(v))) //.runAsync
          }

          def onError(ex: Throwable): Unit = {
            cb.onError(ex)
          }
        }

        (out, AssignableCancelable.dummy)
      }
    }
}
