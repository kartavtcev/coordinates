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

  private var ids: Array[Option[PerId]] = Array(None, None)
  //var first : Option[PerId] = None
  //var second: Option[PerId] = None

  private var meetups: List[Meet] = List.empty

  def getMeetUps = meetups

  def onNext(rec: Record): Unit = {
    val id = rec.id - 1
    val recordHour = Hour(rec.dateTime._2._1)
    val nextHour = Hour(recordHour.value + 1)

    val recordMin = Min(rec.dateTime._2._2)
    val recordFloor = Floor(rec.coordinates._3)

    if (ids(id).isEmpty) {
      ids(id) = Some(
        PerId(List(HourData(recordHour, Map.empty), HourData(nextHour, Map.empty)))) // skipped Date, as it doesn't matter much for the demo data set
    }

    val perId = ids(id).get

/*
    // UNSTUCK events that BOTH occur less then once within a few hours.  TODO: test it.
    if (perId.hours(1).hour.value < recordHour.value) {
      findMeetupsAsync runAsync

      val list = List(HourData(recordHour, Map.empty), HourData(nextHour, Map.empty))
      ids(0) = Some(PerId(list))
      ids(1) = Some(PerId(list))
    }
*/

    if (perId.hours(0).hour == recordHour || perId.hours(1).hour == recordHour) {
      val index = recordHour.value - perId.hours(0).hour.value

      val map = perId.hours(index).perMinCoords
      val recordKey = (recordMin, recordFloor)

      if (map.contains(recordKey)) { // aggregate coordinates per Minute

        val value = map(recordKey)
        val newMap: immutable.Map[(Min, Floor), AvgXY] =
          (map - recordKey) + (recordKey -> AvgXY(value.xSum + rec.coordinates._1, value.ySum + rec.coordinates._2, value.count + 1))
        ids(id) = Some(perId.copy(hours = perId.hours.updated(index, perId.hours(index).copy(perMinCoords = newMap)))) // May be use Optics->Lenses functional design pattern here
      } else {
        val newMap: immutable.Map[(Min, Floor), AvgXY] = map + (recordKey -> AvgXY(rec.coordinates._1, rec.coordinates._2, 1))
        ids(id) = Some(perId.copy(hours = perId.hours.updated(index, perId.hours(index).copy(perMinCoords = newMap))))
      }
    }

    if (perId.hours(1).hour == recordHour && recordMin.value >= Processor.nextHourThreshold) {

      findMeetupsAsync runAsync

      val id1 = ids(0).get
      val list1 = List(id1.hours(1), HourData(nextHour, Map.empty))
      ids(0) = Some(PerId(list1))

      val id2 = ids(1).get
      val list2 = List(id2.hours(1), HourData(nextHour, Map.empty))
      ids(1) = Some(PerId(list2))

    }
  }

  def findMeetupsAsync: Task[Unit] = {
    val id1 = ids(0).get
    val id2 = ids(1).get

    Task { Algorithm.hasMet(id1.hours(0).hour, id1.hours(0).perMinCoords, id2.hours(0).perMinCoords)(Processor.meetUpDistance) }
      .map { m => meetups = meetups ::: m }
  }
}

object Processor {
  val meetUpDistance = 5 // meters
  val nextHourThreshold = 10 // minutes
}

case class PerId(hours: List[HourData])
case class HourData(hour: Hour, perMinCoords: immutable.Map[(Min, Floor), AvgXY])

case class Hour(value: Int)
case class Floor(value: Int)
case class Min(value: Int)
case class AvgXY(xSum: Int, ySum: Int, count: Int)
case class Coordinate(x: Int, y: Int, f: Int)

case class Meet(dateTime: (Hour, Min), coordinates: Coordinate)