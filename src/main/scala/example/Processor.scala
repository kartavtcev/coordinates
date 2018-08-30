package example

import monix.eval.Callback
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber

import scala.collection.immutable

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

  /* TODO: I spent 2 days trying to figure out,
   how to replace "var" / Array (aka mutable Data Structure from Java) id1, id2 with "val" Task[MVar[F]] (Monix) or Ref[IO, F] (Cats).
   Failed so far to restructure code for Task/IO (deferred execution + effects). May be next time. */

  val ids: Array[Option[PerId]] = Array(None, None)
  //var first : Option[PerId] = None
  //var second: Option[PerId] = None

  def onNext(elem: Record): Unit = {
    val id = elem.id - 1
    val recordHour = elem.dateTime._2._1

    if (ids(id).isEmpty) {
      ids(id) = Some(PerId(id, HourlyData(HourData(Hour(recordHour), Map.empty), None), None)) // skipped Date, as it doesn't matter much for the demo data set
    }

    val perId = ids(id).get

    if (perId.current.now.hour.value == recordHour) {
      // TODO:

    } else if (perId.current.now.hour.value < recordHour) {
      if (perId.next.isEmpty) {
        ids(id) = Some(ids(id).get.copy(next = Some(HourlyData(HourData(Hour(recordHour), Map.empty), None))))
      }

      val recordMin = elem.dateTime._2._2
      if (recordMin >= 10) {
        // TODO: timeshift of 10 mins check for exchange + ASYNC processing
      }
      // TODO:

    }
  }

  def hasMet(f: HourlyData, s: HourlyData) : Boolean = { throw new NotImplementedError()}
}

object Processor {

  val aggregateConsumer =
    new Consumer[Record, Any] {

      def createSubscriber(cb: Callback[Any], s: Scheduler) = {
        val out = new Subscriber.Sync[Record] { // Synchronous context, subscribed to single Observable
          implicit val scheduler = s
          private var sum = 0L  //
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

case class PerId(id : Int, current: HourlyData, next: Option[HourlyData], streamingDelay: Min = Min(10))

// iterate throuth hour data: for nearby (hour, min) on the same floor.
// If floor has changed - skip check for 2 coordinates on different floors
// Coordinates per min could be missing
// +10 mins shift for streaming data catch up. => introduce next hour !

case class HourlyData(now: HourData, old: Option[LastCoordFromOldHour])

case class HourData(hour: Hour, perMinCoords: immutable.Map[(Min, Floor), AvgXY])

case class LastCoordFromOldHour(coord: Coordinate)

case class Hour(value: Int)
case class Floor(value: Int)
case class Min(value: Int)
case class AvgXY(xSum: Int, ySum: Int, count: Int)
case class Coordinate(x: Int, y: Int, f: Int)

