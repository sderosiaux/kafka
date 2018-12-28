package kafka.log.cleaner

import kafka.log.{AbortedTxn, TransactionIndex}
import org.apache.kafka.common.record.{ControlRecordType, RecordBatch}

import scala.collection.mutable

/**
 * This is a helper class to facilitate tracking transaction state while cleaning the log. It is initialized
 * with the aborted transactions from the transaction index and its state is updated as the cleaner iterates through
 * the log during a round of cleaning. This class is responsible for deciding when transaction markers can
 * be removed and is therefore also responsible for updating the cleaned transaction index accordingly.
 */
private[log] class CleanedTransactionMetadata(val abortedTransactions: mutable.PriorityQueue[AbortedTxn],
                                              val transactionIndex: Option[TransactionIndex] = None) {
                                                val ongoingCommittedTxns = mutable.Set.empty[Long]
                                                val ongoingAbortedTxns = mutable.Map.empty[Long, AbortedTransactionMetadata]

                                                /**
                                                 * Update the cleaned transaction state with a control batch that has just been traversed by the cleaner.
                                                 * Return true if the control batch can be discarded.
                                                 */
                                                def onControlBatchRead(controlBatch: RecordBatch): Boolean = {
                                                  consumeAbortedTxnsUpTo(controlBatch.lastOffset)

                                                  val controlRecordIterator = controlBatch.iterator
                                                  if (controlRecordIterator.hasNext) {
                                                    val controlRecord = controlRecordIterator.next()
                                                    val controlType = ControlRecordType.parse(controlRecord.key)
                                                    val producerId = controlBatch.producerId
                                                    controlType match {
                                                      case ControlRecordType.ABORT =>
                                                        ongoingAbortedTxns.remove(producerId) match {
                                                          // Retain the marker until all batches from the transaction have been removed
                                                          case Some(abortedTxnMetadata) if abortedTxnMetadata.lastObservedBatchOffset.isDefined =>
                                                            transactionIndex.foreach(_.append(abortedTxnMetadata.abortedTxn))
                                                            false
                                                          case _ => true
                                                        }

                                                      case ControlRecordType.COMMIT =>
                                                        // This marker is eligible for deletion if we didn't traverse any batches from the transaction
                                                        !ongoingCommittedTxns.remove(producerId)

                                                      case _ => false
                                                    }
                                                  } else {
                                                    // An empty control batch was already cleaned, so it's safe to discard
                                                    true
                                                  }
                                                }

                                                private def consumeAbortedTxnsUpTo(offset: Long): Unit = {
                                                  while (abortedTransactions.headOption.exists(_.firstOffset <= offset)) {
                                                    val abortedTxn = abortedTransactions.dequeue()
                                                    ongoingAbortedTxns += abortedTxn.producerId -> new AbortedTransactionMetadata(abortedTxn)
                                                  }
                                                }

                                                /**
                                                 * Update the transactional state for the incoming non-control batch. If the batch is part of
                                                 * an aborted transaction, return true to indicate that it is safe to discard.
                                                 */
                                                def onBatchRead(batch: RecordBatch): Boolean = {
                                                  consumeAbortedTxnsUpTo(batch.lastOffset)
                                                  if (batch.isTransactional) {
                                                    ongoingAbortedTxns.get(batch.producerId) match {
                                                      case Some(abortedTransactionMetadata) =>
                                                        abortedTransactionMetadata.lastObservedBatchOffset = Some(batch.lastOffset)
                                                        true
                                                      case None =>
                                                        ongoingCommittedTxns += batch.producerId
                                                        false
                                                    }
                                                  } else {
                                                    false
                                                  }
                                                }

                                              }

private[log] object CleanedTransactionMetadata {
  def apply(abortedTransactions: List[AbortedTxn],
            transactionIndex: Option[TransactionIndex] = None): CleanedTransactionMetadata = {
    val queue = mutable.PriorityQueue.empty[AbortedTxn](new Ordering[AbortedTxn] {
      override def compare(x: AbortedTxn, y: AbortedTxn): Int = x.firstOffset compare y.firstOffset
    }.reverse)
    queue ++= abortedTransactions
    new CleanedTransactionMetadata(queue, transactionIndex)
  }

  val Empty = CleanedTransactionMetadata(List.empty[AbortedTxn])
}
