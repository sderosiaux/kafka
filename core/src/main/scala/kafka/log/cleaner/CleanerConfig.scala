package kafka.log.cleaner

/**
  * Configuration parameters for the log cleaner
  *
  * @param numThreads The number of cleaner threads to run
  * @param dedupeBufferSize The total memory used for log deduplication
  * @param dedupeBufferLoadFactor The maximum percent full for the deduplication buffer
  * @param maxMessageSize The maximum size of a message that can appear in the log
  * @param maxIoBytesPerSecond The maximum read and write I/O that all cleaner threads are allowed to do
  * @param backOffMs The amount of time to wait before rechecking if no logs are eligible for cleaning
  * @param enableCleaner Allows completely disabling the log cleaner
  * @param hashAlgorithm The hash algorithm to use in key comparison.
  */
case class CleanerConfig(numThreads: Int = 1,
                         dedupeBufferSize: Long = 4 * 1024 * 1024L,
                         dedupeBufferLoadFactor: Double = 0.9d,
                         ioBufferSize: Int = 1024 * 1024,
                         maxMessageSize: Int = 32 * 1024 * 1024,
                         maxIoBytesPerSecond: Double = Double.MaxValue,
                         backOffMs: Long = 15 * 1000,
                         enableCleaner: Boolean = true,
                         hashAlgorithm: String = "MD5") {}
