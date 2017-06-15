package com.lunatech.squeryltools

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException

import scala.util.control.NonFatal
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import org.squeryl.PrimitiveTypeMode.inTransaction

/**
 * Utils for retrying Squeryl transaction when using optimistic locking.
 */
object Transactions {

  private val logger = LoggerFactory.getLogger(this.getClass)

  protected[squeryltools] val _currentSessionDeferreds = new ThreadLocal[List[() => Any]]()

  /**
   * Defer action to the end of the transaction.
   *
   * This is useful for side effects like sending mail; they will only get executed at the end of a transaction.
   * If a transaction is retried because it threw a serialization exception, the deferred actions will only be
   * executed once.
   *
   * If the deferred actions throw an exception (that is not a [RetryableException]), the transaction is rolled
   * back.
   */
  def deferToCommit(action: => Any) =
    _currentSessionDeferreds.get() match {
      case null => throw new IllegalStateException("deferToCommit cannot be used outside a `retryableTransaction` block")
      case existing => _currentSessionDeferreds.set(action _ :: existing)
    }

  /**
   * Retry the provided block up to `retries` times if it fails with a `RetryableException`.
   *
   * The entire block itself is also wrapped in a transaction, to make sure that no race conditions
   * are possible.
   */
  def retryableTransaction[A](retries: Int = 3)(block: => A): A = {

    try {
      _currentSessionDeferreds.set(Nil)
      inTransaction {
        val out = block

        _currentSessionDeferreds.get.reverse.map { action =>
          action()
        }

        out
      }
    } catch {
      case e @ RetryableException(p) if retries > 0 =>
        logger.debug("Caught retryable exception, retrying request. Retries left = " + retries, e)
        retryableTransaction(retries - 1)(block)
      case e @ RetryableException(p) =>
        // We only log at debug level since we rethrow the exception. The client of this library
        // can log it at a higher level if it wants to.
        logger.debug("Caught retryable exception, but ran out of retries. Giving up.", e)
        throw e
      case NonFatal(other) => throw other
    } finally {
      _currentSessionDeferreds.remove()
    }
  }

  /**
   * Retry the provided block if it fails with a `RetryableException`. Invokes the method with the same name
   * but an explicit number of retries with the default number of retries.
   */
  def retryableTransaction[A](block: => A): A = retryableTransaction()(block)

  /**
   * Extractor of retryable exceptions.
   *
   * Postgresql throws `PSQLException`, that have a `getSQLState` method,
   * which contains an error code as found on http://www.postgresql.org/docs/9.1/static/errcodes-appendix.html
   *
   * This object matches on the error codes that we know indicate that the
   * transaction might succeed on retry.
   */
  object RetryableException {
    /**
     * SQL states that indicate that the transaction might succeed if retried.
     * See http://www.postgresql.org/docs/9.1/static/errcodes-appendix.html
     *
     * 40P01 = deadlock detected
     * 40001 = serialization failure
     */
    val retryableSqlStatesPG = Seq("40P01", "40001")
    /**
     * Mysql deadlock SQL state (just like PG, indicates retry might succeed).
     *  See: https://dev.mysql.com/doc/refman/5.7/en/error-messages-server.html#error_er_lock_deadlock
     * 40001 = ER_LOCK_DEADLOCK
     */
    val retryableSqlStatesMySQL = Seq("40001")

    /**
     * Returns true if the provided `Throwable` is caused by a Postgres deadlock exception.
     */
    def apply(t: Throwable): Boolean = t match {
      case e: RuntimeException => t.getCause match {
        case p: PSQLException if retryableSqlStatesPG contains p.getSQLState => true
        case m: MySQLTransactionRollbackException if retryableSqlStatesMySQL contains m.getSQLState => true
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
