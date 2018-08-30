package example

import scala.util.Try

class Parser(val id1: (Int, String), val id2: (Int, String)) {

  def filterPred(line : String) : Boolean = {
    line.endsWith(id1._2) || line.endsWith(id2._2)
  }

  // Creating new DateTime is expensive, just work with strings & split
  // Skip precision in (ms) time & (<1m) coordinates
  // TODO: parser combinators or single regex
  def parse(line: String) : Try[Record] = {
    Try {
      val l = line.split(",")
      val time = (l(0).split("T|\\."))

      val d = time(0).split("-")(2)
      val t = time(1).split(":")

      Record( replaceId(l(4)).get,
        (d.toInt, (t(0).toInt, t(1).toInt, t(2).toInt)),
        (l(1).split("\\.")(0).toInt, l(2).split("\\.")(0).toInt, l(3).toInt) )
    }
  }

  def replaceId(id: String): Option[Int] = {
    if(id == id1._2) Some(id1._1)
    else if(id == id2._2) Some(id2._1)
    else None
  }
}

case class Record(id : Int, dateTime: (Int, (Int, Int, Int)), coordinates: (Int, Int, Int))