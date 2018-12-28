package kafka.log.cleaner

import java.io.IOException
import java.util.concurrent.TimeUnit

import kafka.common.{LogCleaningAbortedException, ThreadShutdownException}
import kafka.log.{Log, SkimpyOffsetMap}
import kafka.server.LogDirFailureChannel
import kafka.utils.{ShutdownableThread, Throttler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.utils.Time

import scala.collection.Iterable
import scala.util.control.ControlThrowable

/**
  * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
  * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
  */
class CleanerThread(threadId: Int,
                    throttler: Throttler,
                    config: CleanerConfig,
                    manager: LogCleanerManager,
                    time: Time,
                    channel: LogDirFailureChannel)
    extends ShutdownableThread(name = "kafka-log-cleaner-thread-" + threadId,
                               isInterruptible = false) {

  protected override def loggerName = classOf[LogCleaner].getName

  if (config.dedupeBufferSize / config.numThreads > Int.MaxValue)
    warn(
      "Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...")

  val cleaner = new Cleaner(
    id = threadId,
    offsetMap = new SkimpyOffsetMap(
      memory = math
        .min(config.dedupeBufferSize / config.numThreads, Int.MaxValue)
        .toInt,
      hashAlgorithm = config.hashAlgorithm),
    ioBufferSize = config.ioBufferSize / config.numThreads / 2,
    maxIoBufferSize = config.maxMessageSize,
    dupBufferLoadFactor = config.dedupeBufferLoadFactor,
    throttler = throttler,
    time = time,
    checkDone = checkDone
  )

  @volatile var lastStats: CleanerStats = new CleanerStats()

  private def checkDone(topicPartition: TopicPartition) {
    if (!isRunning)
      throw new ThreadShutdownException
    manager.checkCleaningAborted(topicPartition)
  }

  /**
    * The main loop for the cleaner thread
    * Clean a log if there is a dirty log available, otherwise sleep for a bit
    */
  override def doWork() {
    val cleaned = cleanFilthiestLog()
    if (!cleaned)
      pause(config.backOffMs, TimeUnit.MILLISECONDS)
  }

  /**
    * Cleans a log if there is a dirty log available
    * @return whether a log was cleaned
    */
  private def cleanFilthiestLog(): Boolean = {
    var currentLog: Option[Log] = None

    try {
      val cleaned = manager.grabFilthiestCompactedLog(time) match {
        case None =>
          false
        case Some(cleanable) =>
          // there's a log, clean it
          currentLog = Some(cleanable.log)
          cleanLog(cleanable)
          true
      }
      val deletable: Iterable[(TopicPartition, Log)] = manager.deletableLogs()
      try {
        deletable.foreach {
          case (topicPartition, log) =>
            try {
              currentLog = Some(log)
              log.deleteOldSegments()
            }
        }
      } finally {
        manager.doneDeleting(deletable.map(_._1))
      }

      cleaned
    } catch {
      case e @ (_: ThreadShutdownException | _: ControlThrowable) => throw e
      case e: Exception =>
        if (currentLog.isEmpty) {
          throw new IllegalStateException(
            "currentLog cannot be empty on an unexpected exception",
            e)
        }
        val erroneousLog = currentLog.get
        warn(
          s"Unexpected exception thrown when cleaning log $erroneousLog. Marking its partition (${erroneousLog.topicPartition}) as uncleanable",
          e)
        manager.markPartitionUncleanable(erroneousLog.dir.getParent,
                                         erroneousLog.topicPartition)

        false
    }
  }

  private def cleanLog(cleanable: LogToClean): Unit = {
    var endOffset = cleanable.firstDirtyOffset
    try {
      val (nextDirtyOffset, cleanerStats) = cleaner.clean(cleanable)
      recordStats(cleaner.id,
                  cleanable.log.name,
                  cleanable.firstDirtyOffset,
                  endOffset,
                  cleanerStats)
      endOffset = nextDirtyOffset
    } catch {
      case _: LogCleaningAbortedException => // task can be aborted, let it go.
      case _: KafkaStorageException       => // partition is already offline. let it go.
      case e: IOException =>
        var logDirectory = cleanable.log.dir.getParent
        val msg =
          s"Failed to clean up log for ${cleanable.topicPartition} in dir ${logDirectory} due to IOException"
        channel.maybeAddOfflineLogDir(logDirectory, msg, e)
    } finally {
      manager.doneCleaning(cleanable.topicPartition,
                           cleanable.log.dir.getParentFile,
                           endOffset)
    }
  }

  /**
    * Log out statistics on a single run of the cleaner.
    */
  def recordStats(id: Int,
                  name: String,
                  from: Long,
                  to: Long,
                  stats: CleanerStats) {
    this.lastStats = stats
    def mb(bytes: Double) = bytes / (1024 * 1024)
    val message =
      "%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n"
        .format(id, name, from, to) +
        "\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n".format(
          mb(stats.bytesRead),
          stats.elapsedSecs,
          mb(stats.bytesRead / stats.elapsedSecs)) +
        "\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n"
          .format(mb(stats.mapBytesRead),
                  stats.elapsedIndexSecs,
                  mb(stats.mapBytesRead) / stats.elapsedIndexSecs,
                  100 * stats.elapsedIndexSecs / stats.elapsedSecs) +
        "\tBuffer utilization: %.1f%%%n".format(100 * stats.bufferUtilization) +
        "\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n"
          .format(
            mb(stats.bytesRead),
            stats.elapsedSecs - stats.elapsedIndexSecs,
            mb(stats.bytesRead) / (stats.elapsedSecs - stats.elapsedIndexSecs),
            100 * (stats.elapsedSecs - stats.elapsedIndexSecs).toDouble / stats.elapsedSecs
          ) +
        "\tStart size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesRead),
                                                         stats.messagesRead) +
        "\tEnd size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesWritten),
                                                       stats.messagesWritten) +
        "\t%.1f%% size reduction (%.1f%% fewer messages)%n".format(
          100.0 * (1.0 - stats.bytesWritten.toDouble / stats.bytesRead),
          100.0 * (1.0 - stats.messagesWritten.toDouble / stats.messagesRead))
    info(message)
    if (stats.invalidMessagesRead > 0) {
      warn(
        "\tFound %d invalid messages during compaction.".format(
          stats.invalidMessagesRead))
    }
  }

}
