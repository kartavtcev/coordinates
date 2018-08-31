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
   Failed (run out of time) so far to restructure code for Task/IO (deferred execution + effects). May be next time.
   Because I use Monix Synchronous Subscriber & subscribed to single Observable, var/Array mutable are OKay here (Pure FP fans would disagree).
   But having Monix Task could have allowed more parallel computing. */

  var ids: Array[Option[PerId]] = Array(None, None)
  //var first : Option[PerId] = None
  //var second: Option[PerId] = None

  def onNext(rec: Record): Unit = {
    val id = rec.id - 1
    val recordHour = Hour(rec.dateTime._2._1)
    val nextHour = Hour(rec.dateTime._2._1 + 1)
    val recordMin = Min(rec.dateTime._2._2)
    val recordFloor = Floor(rec.coordinates._3)

    if (ids(id).isEmpty) {
      ids(id) = Some(PerId(id, HourData(recordHour, Map.empty), HourData(nextHour, Map.empty))) // skipped Date, as it doesn't matter much for the demo data set
    }

    val perId = ids(id).get

    if (perId.current.hour == recordHour) { // Current Hour

      val map = perId.current.perMinCoords
      val recordKey = (recordMin, recordFloor)

      if (map.contains(recordKey)) { // aggregate coordinates per Minute

        val value = map(recordKey)
        val newMap: immutable.Map[(Min, Floor), AvgXY] = (map - recordKey) + (recordKey -> AvgXY(value.xSum + rec.coordinates._1, value.ySum + rec.coordinates._2, value.count + 1))
        ids(id) = Some(perId.copy(current = perId.current.copy(perMinCoords = newMap))) // May be use Optics->Lenses functional design pattern here
      } else {
        val newMap: immutable.Map[(Min, Floor), AvgXY] = map + (recordKey -> AvgXY(rec.coordinates._1, rec.coordinates._2, 1))
        ids(id) = Some(perId.copy(current = perId.current.copy(perMinCoords = newMap)))
      }

    } else if (perId.current.hour.value + 1 == recordHour.value) { // Next Hour
      /*
      if (perId.next.isEmpty) {
        ids(id) = Some(perId.copy(next = Some(HourData(recordHour, Map.empty), None)))
      }*/

      val map = perId.next.perMinCoords
      val recordKey = (recordMin, recordFloor)

      if (map.contains(recordKey)) { // aggregate coordinates per Minute

        val value = map(recordKey)
        val newMap: immutable.Map[(Min, Floor), AvgXY] = (map - recordKey) + (recordKey -> AvgXY(value.xSum + rec.coordinates._1, value.ySum + rec.coordinates._2, value.count + 1))
        ids(id) = Some(perId.copy(next = perId.next.copy(perMinCoords = newMap))) // May be use Optics->Lenses functional design pattern here
      } else {
        val newMap: immutable.Map[(Min, Floor), AvgXY] = map + (recordKey -> AvgXY(rec.coordinates._1, rec.coordinates._2, 1))
        ids(id) = Some(perId.copy(next = perId.next.copy(perMinCoords = newMap)))
      }

      if (recordMin.value >= 10) {
        val id1 = ids(0).get
        val id2 = ids(1).get

        // TODO: ASYNC processing
        // fill/process "Old" field:  last_id - 60
        hasMet(id1, id2)

        ids(0) = Some(PerId(id1.id, id1.next, HourData(Hour(id1.next.hour.value + 1), Map.empty)))
        ids(1) = Some(PerId(id2.id, id2.next, HourData(Hour(id2.next.hour.value + 1), Map.empty)))
      }
    }
  }

  // TODO: process in-PARALLEL
  def hasMet(f: PerId, s: PerId): Unit = {

    def sharedMinuteFloorOrderedDistribution(f: immutable.Map[(Min, Floor), AvgXY], s: immutable.Map[(Min, Floor), AvgXY]): List[(Min, Floor)] = {
      val firstUniqueMin: List[(Min, Floor)] = f.keys.toList
        .sortBy(_._1.value)
        .groupBy(p => p._1.value)
        .map(g => selectFloorByMaxCount(g._2, f))
        .toList
      val secondUniqueMin: List[(Min, Floor)] = s.keys.toList
        .sortBy(_._1.value)
        .groupBy(p => p._1.value)
        .map(g => selectFloorByMaxCount(g._2, s))
        .toList

      throw new NotImplementedError()
    }

    def selectFloorByMaxCount(groups: List[(Min, Floor)], map: immutable.Map[(Min, Floor), AvgXY]): (Min, Floor) = { // Selects the floor with maximum count per minute. Or Any floor if count is equal.
      groups
        .map(g => (g, map(g).count))
        .sortBy(_._2)
        .last
        ._1
    }

    def findCorrespondingTimeAndEqualFloorIntervals(first: List[(Min, Floor)], second: List[(Min, Floor)]): List[((Min, Floor), (Min, Floor))] = {

      def loop(range: List[Int], first: List[(Min, Floor)], second: List[(Min, Floor)]): List[((Min, Floor), (Min, Floor))] = {
        val v1: List[(Min, Floor)] = first.filter(fv => range.exists(_ == fv._1.value))
        val v2: List[(Min, Floor)] = second.filter(sv => range.exists(_ == sv._1.value))

        throw new NotImplementedError()
        /*
        if( && ) {
          val median = range.length / 2
          loop()
        } else {

        }
      }

      loop((0 to 59).toList, first, second)
        .filter { case ((_, floor1),(_, floor2)) => floor1 == floor2 }
      // first: match Minutes
      // second: select equal Floors on matched mins
      */
      }


      val d1 = sharedMinuteFloorOrderedDistribution(f.current.perMinCoords, s.current.perMinCoords)
      val d2 = sharedMinuteFloorOrderedDistribution(f.next.perMinCoords, s.next.perMinCoords)
      throw new NotImplementedError()


    }
    // binary split
  }

  // TODO: data structure of meet-ups on complete


}

object Processor {

  val aggregateConsumer =
    new Consumer[Record, Any] {

      def createSubscriber(cb: Callback[Any], s: Scheduler) = {
        val out = new Subscriber.Sync[Record] {
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

case class PerId(id : Int, current: HourData, next: HourData, streamingDelay: Min = Min(10))

// iterate throuth hour data: for nearby (hour, min) on the same floor.
// If floor has changed - skip check for 2 coordinates on different floors
// Coordinates per min could be missing
// +10 mins shift for streaming data catch up. => introduce next hour !

case class HourData(hour: Hour, perMinCoords: immutable.Map[(Min, Floor), AvgXY])

case class Hour(value: Int)
case class Floor(value: Int)
case class Min(value: Int)
case class AvgXY(xSum: Int, ySum: Int, count: Int)
case class Coordinate(x: Int, y: Int, f: Int)

