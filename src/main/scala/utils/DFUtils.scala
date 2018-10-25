package utils

import org.apache.log4j.Logger
import org.joda.time.format.DateTimeFormat

object DFUtils extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def startsWith(input: String, token: String): Boolean = {
    input.startsWith(token)
  }

  def parseTimestampWithTimezone(input: String): Long = {
    val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    val dtf = DateTimeFormat.forPattern(pattern)
    val dateTime = dtf.parseDateTime(input)
    dateTime.getMillis
  }

  def reverse(str: String): String = {
    str.toList.reverse.mkString
  }

}
