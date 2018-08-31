package example

import monix.eval.Callback
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber

import scala.collection.immutable

class Processor() {

  /* TODO: I spent 2 days trying to figure out,
   how to replace "var" / Array (aka mutable Data Structure from Java) id1, id2 with "val" Task[MVar[F]] (Monix) or Ref[IO, F] (Cats).
   Failed (run out of time) so far to restructure code for Task/IO (deferred execution + effects). May be next time.
   Because I use Monix Synchronous Subscriber & subscribed to single Observable, var/Array mutable are OKay here (Pure FP fans would disagree).
   But having Monix Task could have allowed more parallel computing. */

  var ids: Array[Option[PerId]] = Array(None, None)
  //var first : Option[PerId] = None
  //var second: Option[PerId] = None

  var meetUpsData: List[Meet] = List.empty

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

      if (recordMin.value >= 10) {
        val id1 = ids(0).get
        val id2 = ids(1).get

        // TODO: ASYNC processing
        meetUpsData = meetUpsData ::: hasMet(id1, id2)

        ids(0) = Some(PerId(id1.id, id1.next, HourData(Hour(id1.next.hour.value + 1), Map.empty)))
        ids(1) = Some(PerId(id2.id, id2.next, HourData(Hour(id2.next.hour.value + 1), Map.empty)))
      }
    }
  }

  // TODO: process in-PARALLEL
  def hasMet(firstPerId: PerId, secondPerId: PerId): List[Meet] = {

    def sharedMinuteFloorOrderedDistribution(f: immutable.Map[(Min, Floor), AvgXY],
                                             s: immutable.Map[(Min, Floor), AvgXY]): (List[(Min, Floor)], List[(Min, Floor)]) = {
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

      (firstUniqueMin, secondUniqueMin)
    }

    def selectFloorByMaxCount(groups: List[(Min, Floor)],
                              map: immutable.Map[(Min, Floor), AvgXY]): (Min, Floor) = { // Selects the floor with maximum count per minute. Or Any floor if count is equal.
      groups
        .map(g => (g, map(g).count))
        .sortBy(_._2)
        .last
        ._1
    }

    def findCorrespondingTimeAndEqualFloorIntervals(first: List[(Min, Floor)],
                                                    second: List[(Min, Floor)]): List[((Min, Floor), (Min, Floor))] = {

      def loop(range: List[Int], first: List[(Min, Floor)], second: List[(Min, Floor)]): List[((Min, Floor), (Min, Floor))] = {
        val v1: List[(Min, Floor)] = first.filter(fv => range.exists(_ == fv._1.value))
        val v2: List[(Min, Floor)] = second.filter(sv => range.exists(_ == sv._1.value))

        if (v1.length > 1 && v2.length > 1) {
          val median = range.length / 2
          loop(range.slice(0, median), first, second) ::: loop(range.slice(median, range.length), first, second)
        } else if (v1.length > 1 && v2.length == 1) {
          val v2Val = v2.head
          val v1Vals = v1.filter(_._2 == v2Val._2).sortBy(v => math.abs(v._1.value - v2Val._1.value))
          if (v1Vals.isEmpty) List.empty
          else List((v1Vals.head, v2Val))
        } else if (v1.length == 1 && v2.length > 1) {
          val v1Val = v1.head
          val v2Vals = v2.filter(_._2 == v1Val._2).sortBy(v => math.abs(v._1.value - v1Val._1.value))
          if (v2Vals.isEmpty) List.empty
          else List((v1Val, v2Vals.head))
        } else if (v1.length == 1 && v2.length == 1) {
          if (v1.head._2 == v2.head._2) List((v1.head, v2.head))
          else List.empty
        } else List.empty
      }

      loop((0 to 59).toList, first, second)
    }

    def distanceCheck(coords: List[((Min, Floor), AvgXY, AvgXY)]): List[Meet] = {
      def isDistanceMeet(x1: Double, y1: Double, x2: Double, y2: Double) : Boolean = {
        scala.math.pow((x2 - x1), 2) + scala.math.pow((y2 - y1), 2) <= scala.math.pow(Processor.meetUpDistance, 2)
      }

      var meets: List[Meet] = List.empty

      for(coord <- coords) {
        val key1 = coord._1
        val avgXY1 = coord._2
        val avgXY2 = coord._3


        val (min, Floor(floor)) = key1
        val AvgXY(x1Sum, y1Sum, count1) = avgXY1
        val AvgXY(x2Sum, y2Sum, count2) = avgXY2

        val x1 = x1Sum.toDouble / count1.toDouble
        val y1 = y1Sum.toDouble / count1.toDouble

        val x2 = x2Sum.toDouble / count2.toDouble
        val y2 = y2Sum.toDouble / count2.toDouble

        if(isDistanceMeet(x1, y1, x2, y2)) {
          val xMed = (x1 + x2) / 2.0
          val yMed = (y1 + y2) / 2.0

          meets = meets :+ Meet((firstPerId.current.hour, min), Coordinate(xMed.toInt, yMed.toInt, floor))
        }
      }
      meets
    }

    val d1 = sharedMinuteFloorOrderedDistribution(firstPerId.current.perMinCoords, secondPerId.current.perMinCoords)
    val d2 = sharedMinuteFloorOrderedDistribution(firstPerId.next.perMinCoords, secondPerId.next.perMinCoords)

    val distanceCheckBase = findCorrespondingTimeAndEqualFloorIntervals(d1._1, d1._2)
    val distanceCheckNextSingle = findCorrespondingTimeAndEqualFloorIntervals(d2._1, d2._2)

    var distanceCheckCoords : List[((Min, Floor), AvgXY, AvgXY)] =
      distanceCheckBase.map { case (key1, key2) => (key1, firstPerId.current.perMinCoords(key1), secondPerId.current.perMinCoords(key2)) }   // + HOUR
    if(!distanceCheckNextSingle.isEmpty) {
      val distanceNextSingle =
        distanceCheckNextSingle.map { case (key1, key2) => (key1, firstPerId.next.perMinCoords(key1), secondPerId.next.perMinCoords(key2)) } .head
      distanceCheckCoords = distanceCheckCoords :+ distanceNextSingle
    }

    distanceCheck(distanceCheckCoords)
  }

}

object Processor {
  val meetUpDistance = 5 // meters

  val aggregateConsumer =
    new Consumer[Record, List[Meet]] {

      def createSubscriber(cb: Callback[List[Meet]], s: Scheduler) = {
        val out = new Subscriber.Sync[Record] {
          implicit val scheduler = s
          val processor = new Processor()

          def onNext(elem: Record) = {
            processor.onNext(elem)
            Continue
          }

          def onComplete(): Unit = {
            // TODO: LAST HOUR PROCESS !!!
            cb.onSuccess(processor.meetUpsData)
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
case class HourData(hour: Hour, perMinCoords: immutable.Map[(Min, Floor), AvgXY])

case class Hour(value: Int)
case class Floor(value: Int)
case class Min(value: Int)
case class AvgXY(xSum: Int, ySum: Int, count: Int)
case class Coordinate(x: Int, y: Int, f: Int)

case class Meet(dateTime: (Hour, Min), coordinates: Coordinate)