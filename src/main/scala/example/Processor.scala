package example

import monix.eval.{Callback}
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber

//class Processor() {    // return FIRST coords intersection on data   "your code should indicate if a meeting has occurred"

  // first coords

  // last coord from PREV periodv
  //val startDate: [(Int, (Int, Int))]
  //val startHourDataPerMin: [Array[(Int, (Int, Int, Int))]]   // array is per-minute ! 0-59 ...
  // count of Hours
  //val lastCoordFromPrevHour: (Int, (Int, Int, Int))

  // TODO: do 1 min shift for old data in a stream ... while consuming a new hour.
  // TODO: THEN PROCESS AN OLD DATA & MOVE TO NEW ONE. CURRENT = NEW ...
  // next hour
  //

  // TODO: second coords

  // PROCESS PARALLEL HOURDATA

class Processor() {
}

object Processor {

  val aggregateConsumer =
    new Consumer[Record, Any] {

      def createSubscriber(cb: Callback[Any], s: Scheduler) = {
        val out = new Subscriber.Sync[Record] {
          implicit val scheduler = s
          private var sum = 0L
          val processor = new Processor()

          def onNext(elem: Record) = {
            sum += elem.id
            Continue
          }

          def onComplete(): Unit = {
            cb.onSuccess(sum)
          }

          def onError(ex: Throwable): Unit = {
            cb.onError(ex)
          }
        }

        (out, AssignableCancelable.dummy)
      }
    }
}
