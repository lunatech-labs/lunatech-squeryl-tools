package squeryltools

import org.postgresql.util.PSQLException
import org.squeryl.PrimitiveTypeMode._
import scala.util.control.NonFatal

object DbUtil {

  /**
   * Retry the provided block up to `retries` times if it fails with a `RetryableException`.
   *
   * The entire block itself is also wrapped in a transaction, to make sure that no race conditions
   * are possible.
   */
  def retryableTransaction[A](retries: Int = 3)(block: => A): A = {
    try {
      inTransaction { block }
    } catch {
      case RetryableException(p) if retries > 0 =>
        // TODO, use a suitable logging API
        println("Caught deadlock exception, retrying request. Retries left = " + retries)
        retryableTransaction(retries - 1)(block)
      case e@RetryableException(p) =>
        // TODO, use a suitable logging API
        println("Caught deadlock exception, but ran out of retries. Giving up.")
        throw e
      case NonFatal(other) => throw other
    }
  }

  /**
   * Retry the provided block if it fails with a `RetryableException`. Invokes the method with the same name
   * but an explicit number of retries with the default number of retries.
   */
  def retryableTransaction[A](block: => A): A = retryableTransaction()(block)

  /**
   * Extractor of retryable exceptions.
   */
  object RetryableException {
    val deadlockStates = Seq("40P01", "40001")

    /**
     * Returns true if the provided `Throwable` is caused by a Postgres deadlock exception.
     */
    def apply(t: Throwable): Boolean = t match {
      case e: RuntimeException => t.getCause match {
        case p: PSQLException if deadlockStates contains p.getSQLState => true
        case _ => false
      }
      case _ => false
    }

    /**
     * Returns Some(t) if RetryableException(t) == true, otherwise None
     */
    def unapply(t: Throwable) = if (apply(t)) Some(t) else None
  }
}
