package example

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import scala.collection.immutable

class Processor(implicit val ctx: monix.execution.Scheduler) extends StrictLogging {

  /* TODO: I spent 2 days trying to figure out,
   how to replace "var" / Array (aka mutable Data Structure from Java) id1, id2 with "val" Task[MVar[F]] (Monix) or Ref[IO, F] (Cats).
   Failed (run out of time) so far to restructure code for Task/IO (deferred execution + effects). May be next time.
   Because I use Monix Synchronous Subscriber & subscribed to single Observable, var/Array mutable are OKay here (Pure FP fans would disagree).
   But having Monix Task could have allowed more parallel computing. */

  var ids: Array[Option[PerId]] = Array(None, None)
  //var first : Option[PerId] = None
  //var second: Option[PerId] = None

  var meetups: List[Meet] = List.empty

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
        val newMap: immutable.Map[(Min, Floor), AvgXY] =
          (map - recordKey) + (recordKey -> AvgXY(value.xSum + rec.coordinates._1, value.ySum + rec.coordinates._2, value.count + 1))
        ids(id) = Some(perId.copy(current = perId.current.copy(perMinCoords = newMap))) // May be use Optics->Lenses functional design pattern here
      } else {
        val newMap: immutable.Map[(Min, Floor), AvgXY] = map + (recordKey -> AvgXY(rec.coordinates._1, rec.coordinates._2, 1))
        ids(id) = Some(perId.copy(current = perId.current.copy(perMinCoords = newMap)))
      }

    } else if (perId.current.hour.value + 1 == recordHour.value) { // Next Hour

      val map = perId.next.perMinCoords
      val recordKey = (recordMin, recordFloor)

      if (map.contains(recordKey)) { // aggregate coordinates per Minute

        val value = map(recordKey)
        val newMap: immutable.Map[(Min, Floor), AvgXY] =
          (map - recordKey) + (recordKey -> AvgXY(value.xSum + rec.coordinates._1, value.ySum + rec.coordinates._2, value.count + 1))
        ids(id) = Some(perId.copy(next = perId.next.copy(perMinCoords = newMap))) // May be use Optics->Lenses functional design pattern here
      } else {
        val newMap: immutable.Map[(Min, Floor), AvgXY] = map + (recordKey -> AvgXY(rec.coordinates._1, rec.coordinates._2, 1))
        ids(id) = Some(perId.copy(next = perId.next.copy(perMinCoords = newMap)))
      }

      if (recordMin.value >= Processor.nextHourThreshold) {

        findMeetupsAsync runAsync

        val id1 = ids(0).get
        val id2 = ids(1).get
        ids(0) = Some(PerId(id1.id, id1.next, HourData(Hour(id1.next.hour.value + 1), Map.empty)))
        ids(1) = Some(PerId(id2.id, id2.next, HourData(Hour(id2.next.hour.value + 1), Map.empty)))
      }
    }
  }

  def findMeetupsAsync: Task[Unit] = {
    val id1 = ids(0).get
    val id2 = ids(1).get

    // asynchronously, run deferred task just like eager Future, because Monix.
      Task { Algorithm.hasMet(id1, id2)(Processor.meetUpDistance, Processor.nextHourThreshold) } map { m => meetups = meetups ::: m }
  }
}

object Processor {
  val meetUpDistance = 5 // meters
  val nextHourThreshold = 10 // minutes
}

case class PerId(id : Int, current: HourData, next: HourData, streamingDelay: Min = Min(10))
case class HourData(hour: Hour, perMinCoords: immutable.Map[(Min, Floor), AvgXY])

sealed trait HourTiming
object Current extends HourTiming
object Next extends HourTiming

case class Hour(value: Int)
case class Floor(value: Int)
case class Min(value: Int)
case class AvgXY(xSum: Int, ySum: Int, count: Int)
case class Coordinate(x: Int, y: Int, f: Int)

case class Meet(dateTime: (Hour, Min), coordinates: Coordinate)