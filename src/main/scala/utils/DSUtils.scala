package utils

import org.apache.log4j.Logger
import schemas.User

object DSUtils extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def createPersonFromString(rawString: String): User = {
    logger.info("people from string")
    val split = rawString.split(",")

    if (split.length < 2) {
      logger.warn("not enough fields in this string")
      User(null, null)
    } else if (split.length == 2) {
      User(split(0), split(1))
    } else {
      logger.warn("Too many fields in this string")
      User(null, null)
    }
  }
}
