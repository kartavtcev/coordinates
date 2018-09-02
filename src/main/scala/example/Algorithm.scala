package example

import scala.collection.immutable

object Algorithm {
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
                                                    second: List[(Min, Floor)],
                                                    timing: HourTiming): List[((Min, Floor), (Min, Floor))] = {

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

      timing match {
        case Current => loop((0 to 59).toList, first, second)
        case Next => loop((0 to Processor.nextHourThreshold).toList, first, second)
      }
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

          /*println(s"1: ($x1,$y1); 2: ($x2,$y2)")*/

          val xMed = (x1 + x2) / 2.0
          val yMed = (y1 + y2) / 2.0

          meets = meets :+ Meet((firstPerId.current.hour, min), Coordinate(xMed.toInt, yMed.toInt, floor))
        }
      }
      meets
    }

    val d1 = sharedMinuteFloorOrderedDistribution(firstPerId.current.perMinCoords, secondPerId.current.perMinCoords)
    val d2 = sharedMinuteFloorOrderedDistribution(firstPerId.next.perMinCoords, secondPerId.next.perMinCoords)

    val distanceCheckBase = findCorrespondingTimeAndEqualFloorIntervals(d1._1, d1._2, Current)
    val distanceCheckNextSingle = findCorrespondingTimeAndEqualFloorIntervals(d2._1, d2._2, Next)

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
