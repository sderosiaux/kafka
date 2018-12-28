package kafka.log.cleaner

import kafka.log.Log
import org.apache.kafka.common.TopicPartition

/**
 * Helper class for a log, its topic/partition, the first cleanable position, and the first uncleanable dirty position
 */
private case class LogToClean(topicPartition: TopicPartition, log: Log, firstDirtyOffset: Long, uncleanableOffset: Long) extends Ordered[LogToClean] {
  val cleanBytes = log.logSegments(-1, firstDirtyOffset).map(_.size.toLong).sum
  val (firstUncleanableOffset, cleanableBytes) = LogCleaner.calculateCleanableBytes(log, firstDirtyOffset, uncleanableOffset)
  val totalBytes = cleanBytes + cleanableBytes
  val cleanableRatio = cleanableBytes / totalBytes.toDouble
  override def compare(that: LogToClean): Int = math.signum(this.cleanableRatio - that.cleanableRatio).toInt
}
