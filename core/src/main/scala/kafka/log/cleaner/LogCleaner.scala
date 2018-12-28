/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log.cleaner

import java.io.{File, IOException}
import java.nio._
import java.util.Date
import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Gauge
import kafka.common._
import kafka.log.{AbortedTxn, Log, LogSegment, TransactionIndex}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerReconfigurable, KafkaConfig, LogDirFailureChannel}
import kafka.utils._
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.{CorruptRecordException, KafkaStorageException}
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Iterable, Set, mutable}
import scala.util.control.ControlThrowable

/**
 * The cleaner is responsible for removing obsolete records from logs which have the "compact" retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 *
 * Each log can be thought of being split into two sections of segments: a "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The dirty section is further divided into the "cleanable" section followed by an "uncleanable" section.
 * The uncleanable section is excluded from cleaning. The active log segment is always uncleanable. If there is a
 * compaction lag time set, segments whose largest message timestamp is within the compaction lag time of the cleaning operation are also uncleanable.
 *
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "compact" retention policy
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log.
 *
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping.
 *
 * Once the key=>last_offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 *
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 *
 * Cleaned segments are swapped into the log as they become available.
 *
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 *
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner.
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 *
 * Note that cleaning is more complicated with the idempotent/transactional producer capabilities. The following
 * are the key points:
 *
 * 1. In order to maintain sequence number continuity for active producers, we always retain the last batch
 *    from each producerId, even if all the records from the batch have been removed. The batch will be removed
 *    once the producer either writes a new batch or is expired due to inactivity.
 * 2. We do not clean beyond the last stable offset. This ensures that all records observed by the cleaner have
 *    been decided (i.e. committed or aborted). In particular, this allows us to use the transaction index to
 *    collect the aborted transactions ahead of time.
 * 3. Records from aborted transactions are removed by the cleaner immediately without regard to record keys.
 * 4. Transaction markers are retained until all record batches from the same transaction have been removed and
 *    a sufficient amount of time has passed to reasonably ensure that an active consumer wouldn't consume any
 *    data from the transaction prior to reaching the offset of the marker. This follows the same logic used for
 *    tombstone deletion.
 *
 * @param initialConfig Initial configuration parameters for the cleaner. Actual config may be dynamically updated.
 * @param logDirs The directories where offset checkpoints reside
 * @param logs The pool of logs
 * @param time A way to control the passage of time
 */
class LogCleaner(initialConfig: CleanerConfig,
                 val logDirs: Seq[File],
                 val logs: Pool[TopicPartition, Log],
                 val logDirFailureChannel: LogDirFailureChannel,
                 time: Time = Time.SYSTEM) extends Logging with KafkaMetricsGroup with BrokerReconfigurable
{

  /* Log cleaner configuration which may be dynamically updated */
  @volatile private var config = initialConfig

  /* for managing the state of partitions being cleaned. package-private to allow access in tests */
  private[log] val cleanerManager = new LogCleanerManager(logDirs, logs, logDirFailureChannel)

  /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
  private val throttler = new Throttler(desiredRatePerSec = config.maxIoBytesPerSecond,
                                        checkIntervalMs = 300,
                                        throttleDown = true,
                                        "cleaner-io",
                                        "bytes",
                                        time = time)

  /* the threads */
  private val cleaners = mutable.ArrayBuffer[CleanerThread]()

  /**
   * Start the background cleaning
   */
  def startup(): Unit = {
    info("Starting the log cleaner")
    cleaners ++= (0 until config.numThreads).map(i => new CleanerThread(i, throttler, config, cleanerManager, time, logDirFailureChannel))
    cleaners.foreach(_.start)
  }

  /**
   * Stop the background cleaning
   */
  def shutdown() {
    info("Shutting down the log cleaner.")
    cleaners.foreach(_.shutdown())
    cleaners.clear()
  }

  override def reconfigurableConfigs: Set[String] = {
    LogCleaner.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    val newCleanerConfig = LogCleaner.cleanerConfig(newConfig)
    val numThreads = newCleanerConfig.numThreads
    val currentThreads = config.numThreads
    if (numThreads < 1)
      throw new ConfigException(s"Log cleaner threads should be at least 1")
    if (numThreads < currentThreads / 2)
      throw new ConfigException(s"Log cleaner threads cannot be reduced to less than half the current value $currentThreads")
    if (numThreads > currentThreads * 2)
      throw new ConfigException(s"Log cleaner threads cannot be increased to more than double the current value $currentThreads")

  }

  /**
    * Reconfigure log clean config. This simply stops current log cleaners and creates new ones.
    * That ensures that if any of the cleaners had failed, new cleaners are created to match the new config.
    */
  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    config = LogCleaner.cleanerConfig(newConfig)
    shutdown()
    startup()
  }

  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   */
  def abortCleaning(topicPartition: TopicPartition) {
    cleanerManager.abortCleaning(topicPartition)
  }

  /**
   * Update checkpoint file, removing topics and partitions that no longer exist
   */
  def updateCheckpoints(dataDir: File) {
    cleanerManager.updateCheckpoints(dataDir, update=None)
  }

  def alterCheckpointDir(topicPartition: TopicPartition, sourceLogDir: File, destLogDir: File): Unit = {
    cleanerManager.alterCheckpointDir(topicPartition, sourceLogDir, destLogDir)
  }

  def handleLogDirFailure(dir: String) {
    cleanerManager.handleLogDirFailure(dir)
  }

  /**
   * Truncate cleaner offset checkpoint for the given partition if its checkpointed offset is larger than the given offset
   */
  def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long) {
    cleanerManager.maybeTruncateCheckpoint(dataDir, topicPartition, offset)
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition) {
    cleanerManager.abortAndPauseCleaning(topicPartition)
  }

  /**
    *  Resume the cleaning of paused partitions.
    */
  def resumeCleaning(topicPartitions: Iterable[TopicPartition]) {
    cleanerManager.resumeCleaning(topicPartitions)
  }

  /**
   * For testing, a way to know when work has completed. This method waits until the
   * cleaner has processed up to the given offset on the specified topic/partition
   *
   * @param topicPartition The topic and partition to be cleaned
   * @param offset The first dirty offset that the cleaner doesn't have to clean
   * @param maxWaitMs The maximum time in ms to wait for cleaner
   *
   * @return A boolean indicating whether the work has completed before timeout
   */
  def awaitCleaned(topicPartition: TopicPartition, offset: Long, maxWaitMs: Long = 60000L): Boolean = {
    def isCleaned = cleanerManager.allCleanerCheckpoints.get(topicPartition).fold(false)(_ >= offset)
    var remainingWaitMs = maxWaitMs
    while (!isCleaned && remainingWaitMs > 0) {
      val sleepTime = math.min(100, remainingWaitMs)
      Thread.sleep(sleepTime)
      remainingWaitMs -= sleepTime
    }
    isCleaned
  }

  /**
    * To prevent race between retention and compaction,
    * retention threads need to make this call to obtain:
    * @return A list of log partitions that retention threads can safely work on
    */
  def pauseCleaningForNonCompactedPartitions(): Iterable[(TopicPartition, Log)] = {
    cleanerManager.pauseCleaningForNonCompactedPartitions()
  }

  // Only for testing
  private[kafka] def currentConfig: CleanerConfig = config

  // Only for testing
  private[log] def cleanerCount: Int = cleaners.size


}

object LogCleaner {
  val ReconfigurableConfigs = Set(
    KafkaConfig.LogCleanerThreadsProp,
    KafkaConfig.LogCleanerDedupeBufferSizeProp,
    KafkaConfig.LogCleanerDedupeBufferLoadFactorProp,
    KafkaConfig.LogCleanerIoBufferSizeProp,
    KafkaConfig.MessageMaxBytesProp,
    KafkaConfig.LogCleanerIoMaxBytesPerSecondProp,
    KafkaConfig.LogCleanerBackoffMsProp
  )

  def cleanerConfig(config: KafkaConfig): CleanerConfig = {
    CleanerConfig(numThreads = config.logCleanerThreads,
      dedupeBufferSize = config.logCleanerDedupeBufferSize,
      dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
      ioBufferSize = config.logCleanerIoBufferSize,
      maxMessageSize = config.messageMaxBytes,
      maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
      backOffMs = config.logCleanerBackoffMs,
      enableCleaner = config.logCleanerEnable)

  }

  def createNewCleanedSegment(log: Log, baseOffset: Long): LogSegment = {
    LogSegment.deleteIfExists(log.dir, baseOffset, fileSuffix = Log.CleanedFileSuffix)
    LogSegment.open(log.dir, baseOffset, log.config, Time.SYSTEM, fileAlreadyExists = false,
      fileSuffix = Log.CleanedFileSuffix, initFileSize = log.initFileSize, preallocate = log.config.preallocate)
  }

  /**
    * Given the first dirty offset and an uncleanable offset, calculates the total cleanable bytes for this log
    * @return the biggest uncleanable offset and the total amount of cleanable bytes
    */
  def calculateCleanableBytes(log: Log, firstDirtyOffset: Long, uncleanableOffset: Long): (Long, Long) = {
    val firstUncleanableSegment = log.logSegments(uncleanableOffset, log.activeSegment.baseOffset).headOption.getOrElse(log.activeSegment)
    val firstUncleanableOffset = firstUncleanableSegment.baseOffset
    val cleanableBytes = log.logSegments(firstDirtyOffset, math.max(firstDirtyOffset, firstUncleanableOffset)).map(_.size.toLong).sum

    (firstUncleanableOffset, cleanableBytes)
  }
}

private class AbortedTransactionMetadata(val abortedTxn: AbortedTxn) {
  var lastObservedBatchOffset: Option[Long] = None
}
