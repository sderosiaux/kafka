package kafka.log.cleaner

import kafka.log.{Log, LogSegment}
import org.apache.kafka.common.TopicPartition

/**
 * Helper class for a log, its topic/partition, the first cleanable position, and the first uncleanable dirty position
 */
private case class LogToClean(tp: TopicPartition, log: Log, firstDirtyOffset: Long, uncleanableOffset: Long) {

  val firstUncleanableOffset: Long = log
    .logSegments(uncleanableOffset, log.activeSegment.baseOffset)
    .headOption
    .getOrElse(log.activeSegment)
    .baseOffset

  val cleanableBytes: Long = log
    .logSegments(firstDirtyOffset, math.max(firstDirtyOffset, firstUncleanableOffset))
    .map(_.size.toLong)
    .sum

  val cleanBytes: Long = log.logSegments(-1, firstDirtyOffset).map(_.size.toLong).sum

  val totalBytes: Long = cleanBytes + cleanableBytes

  val cleanableRatio: Double = cleanableBytes / totalBytes.toDouble

}

object LogToClean {
  implicit val order: Ordering[LogToClean] = (x, y) => math.signum(x.cleanableRatio - y.cleanableRatio).toInt
}
