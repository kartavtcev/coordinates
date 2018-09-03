package example


import org.scalatest._

import scala.util.Success

class ParserSpec extends FlatSpec with Matchers {
  "Parser " should "parse input data string to correct DTO" in {

    val id1 = (1, "600dfbe2")
    val id2 = (2, "5e7b40e1")
    val parser = new Parser(id1, id2)

    val toParse1 = "2014-07-19T16:00:06.071Z,103.79211,71.50419417988532,1,600dfbe2"
    val toParse2 = "2014-07-19T16:00:06.074Z,110.33613,100.6828393188978,1,5e7b40e1"

    parser.parse(toParse1) shouldEqual Success(Record(id1._1, (19, (16, 0, 6)), (103, 71, 1)))
    parser.parse(toParse2) shouldEqual Success(Record(id2._1, (19, (16, 0, 6)), (110, 100, 1)))
  }
}
