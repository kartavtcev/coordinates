package example

import monix.eval.Callback
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber

class Processor {    // return FIRST coords intersection on data   "your code should indicate if a meeting has occurred"

  // first coords

  // last coord from PREV periodv
  //val startDate: MVar[(Int, (Int, Int))] = MVar.empty
  //val startHourDataPerMin: MVar[Array[(Int, (Int, Int, Int))]] = MVar.empty   // array is per-minute ! 0-59 ...
  // count of Hours
  //val lastCoordFromPrevHour: (Int, (Int, Int, Int))

  // TODO: do 1 min shift for old data in a stream ... while consuming a new hour.
  // TODO: THEN PROCESS AN OLD DATA & MOVE TO NEW ONE. CURRENT = NEW ...
  // next hour
  //

  // TODO: second coords

  // PROCESS PARALLEL HOURDATA

}

object Processor {

  val aggregateConsumer =
    new Consumer[Tuple3[Int, Tuple2[Int, Tuple3[Int, Int, Int]], Tuple3[Int, Int, Int]], Any] {

      def createSubscriber(cb: Callback[Any], s: Scheduler) = {
        val out = new Subscriber.Sync[Tuple3[Int, Tuple2[Int, Tuple3[Int, Int, Int]], Tuple3[Int, Int, Int]]] {
          implicit val scheduler = s
          private var sum = 0L

          def onNext(elem: Tuple3[Int, Tuple2[Int, Tuple3[Int, Int, Int]], Tuple3[Int, Int, Int]]) = {
            sum += 1L//elem._1
            Continue
          }

          def onComplete(): Unit = {
            // We're done so we can signal the final result
            cb.onSuccess(sum)
          }

          def onError(ex: Throwable): Unit = {
            // Error happened, so we signal the error
            cb.onError(ex)
          }
        }

        // return subscriber & a dummy Cancelable as not used
        (out, AssignableCancelable.dummy)
      }
    }
}
