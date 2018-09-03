package example


import org.scalatest._

import scala.collection.immutable

class AlgorithmSpec extends FlatSpec with Matchers {
  "Algorithm " should "find a meetup on the same floor on sparsened data (choose nearby coordinate)" in {

    val hour = Hour(1)
    val firstId : immutable.Map[(Min, Floor), AvgXY] =
      Map((Min(30), Floor(1)) -> AvgXY(5, 5, 1), (Min(31), Floor(1)) -> AvgXY(5, 6, 1))
    val secondId: immutable.Map[(Min, Floor), AvgXY] =
      Map((Min(35), Floor(1)) -> AvgXY(6, 5, 1))

    val result = Algorithm.hasMet(hour, firstId, secondId)(Processor.meetUpDistance)
    result shouldEqual List(Meet((Hour(1), Min(33)), Coordinate(5,5,1)))
  }

  "Algorithm " should "find a meetup on the same floor on sparsened data" in {

    val hour = Hour(1)
    val firstId : immutable.Map[(Min, Floor), AvgXY] =
      Map((Min(30), Floor(1)) -> AvgXY(5, 7, 1), (Min(31), Floor(2)) -> AvgXY(5, 6, 1))
    val secondId: immutable.Map[(Min, Floor), AvgXY] =
      Map((Min(35), Floor(1)) -> AvgXY(6, 5, 1))

    val result = Algorithm.hasMet(hour, firstId, secondId)(Processor.meetUpDistance)
    result shouldEqual List(Meet((Hour(1), Min(32)), Coordinate(5,6,1)))
  }

  "Algorithm " should "not find a meetup on different floors" in {

    val hour = Hour(1)
    val firstId : immutable.Map[(Min, Floor), AvgXY] =
      Map((Min(30), Floor(2)) -> AvgXY(5, 7, 1), (Min(31), Floor(2)) -> AvgXY(5, 6, 1))
    val secondId: immutable.Map[(Min, Floor), AvgXY] =
      Map((Min(35), Floor(1)) -> AvgXY(6, 5, 1))

    val result = Algorithm.hasMet(hour, firstId, secondId)(Processor.meetUpDistance)
    result shouldEqual List()
  }

  "Algorithm " should "filter a floor with maximum count per minute" in {

    val hour = Hour(1)
    val firstId : immutable.Map[(Min, Floor), AvgXY] =
      Map(
        (Min(30), Floor(2)) -> AvgXY(5, 7, 1),
        (Min(30), Floor(1)) -> AvgXY(10, 10, 2),
        (Min(31), Floor(2)) -> AvgXY(5, 6, 1))
    val secondId: immutable.Map[(Min, Floor), AvgXY] =
      Map((Min(35), Floor(1)) -> AvgXY(6, 5, 1))

    val result = Algorithm.hasMet(hour, firstId, secondId)(Processor.meetUpDistance)
    result shouldEqual List(Meet((Hour(1), Min(32)), Coordinate(5,5,1)))
  }
}