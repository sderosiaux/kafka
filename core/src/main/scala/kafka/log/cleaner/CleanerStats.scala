package kafka.log.cleaner

import org.apache.kafka.common.utils.Time

/**
 * A simple struct for collecting stats about log cleaning
 */
private class CleanerStats(time: Time = Time.SYSTEM) {
  val startTime = time.milliseconds
  var mapCompleteTime = -1L
  var endTime = -1L
  var bytesRead = 0L
  var bytesWritten = 0L
  var mapBytesRead = 0L
  var mapMessagesRead = 0L
  var messagesRead = 0L
  var invalidMessagesRead = 0L
  var messagesWritten = 0L
  var bufferUtilization = 0.0d

  def readMessages(messagesRead: Int, bytesRead: Int) {
    this.messagesRead += messagesRead
    this.bytesRead += bytesRead
  }

  def invalidMessage() {
    invalidMessagesRead += 1
  }

  def recopyMessages(messagesWritten: Int, bytesWritten: Int) {
    this.messagesWritten += messagesWritten
    this.bytesWritten += bytesWritten
  }

  def indexMessagesRead(size: Int) {
    mapMessagesRead += size
  }

  def indexBytesRead(size: Int) {
    mapBytesRead += size
  }

  def indexDone() {
    mapCompleteTime = time.milliseconds
  }

  def allDone() {
    endTime = time.milliseconds
  }

  def elapsedSecs = (endTime - startTime)/1000.0

  def elapsedIndexSecs = (mapCompleteTime - startTime)/1000.0

}
