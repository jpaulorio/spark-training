import org.joda.time.DateTime
import com.thoughtworks.utils.DFUtils._

val timestamp = parseTimestampWithTimezone("2018-08-28T17:06:17.181-03:00")

val datetime = new DateTime(timestamp)

datetime.toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

val range = 1 to 0

range.size

