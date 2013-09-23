package com.lunatech.squeryltools

import java.sql.Connection
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.specs2.mutable.{After, Specification}
import org.specs2.specification.Scope
import org.squeryl.{Schema, Session, SessionFactory}
import org.squeryl.PrimitiveTypeMode.{__thisDsl, binaryOpConv12, inTransaction, int2ScalarInt, setAll, update, view2QueryAll}
import org.squeryl.adapters.PostgreSqlAdapter

import com.lunatech.squeryltools.Transactions.{RetryableException, deferToCommit, retryableTransaction}

class TransactionsSpec extends Specification {
  "TransactionsSpec".title

  "All database transactions in this specification run with the SERIALIZABLE isolation level".txt

  // Many examples in this specification use the same table in an external Postgres database,
  // so they must be run sequentially
  sequential

  val futureTimeout = Duration(1, TimeUnit.SECONDS)

  // Create the schema
  step {
    new ConnectionScope {
      inTransaction {
        TestSchema.create
      }
    }
  }

  "2 interlocking transactions" should {

    // TODO, make a matcher that can inspect cause exceptions to find the real postgres exception
    "cause one to throw an exception containing 'sqlState: 40001' in the message and abort" in new SampleDataScope {

      // We use a latch to synchronize the transactions and to make sure they can't be serialized
      val latch = new CountDownLatch(2)

      val results = Future.sequence(Seq(
        Future(readAndWaitForUpdate(latch)),
        Future(readAndWaitForUpdate(latch))))

      // One of the transactions must fail
      Await.result(results, futureTimeout) must throwAn[Exception].like {
        case e =>
          e.getMessage() must contain("sqlState: 40001")
      }

      // Exactly one of the two transactions must have succeeded and updated the value
      currentRecordValue must_== 1
    }
  }

  "2 interlocking transactions in a `retryableTransaction` block" should {

    // TODO, try to confirm that it actually did something, maybe by checking the logging output somehow?
    inExample("complete successfully") in new SampleDataScope {

      val latch = new CountDownLatch(2)

      val results = Future.sequence(Seq(
        Future(retryableTransaction(readAndWaitForUpdate(latch))),
        Future(retryableTransaction(readAndWaitForUpdate(latch)))))

      Await.result(results, futureTimeout) must have size (2)

      // Both transactions must have succeeded and updated the value
      currentRecordValue must_== 2

    }
  }

  "A slow transaction in a `retryableTransaction` block that keeps failing because of fast other transactions" should {

    // TODO, try to confirm that it actually did something, maybe by checking the logging output somehow?
    inExample("eventually give up and throw the exception") in new SampleDataScope {

      // This is our slow transaction
      val slowTransaction = Future {
        retryableTransaction {
          queryAllRecords

          // Now, in between comes a fast transaction that commits first
          val fastTransaction = Future {
            inTransaction {
              queryAllRecords
              updateAllRecords()
            }
          }
          Await.result(fastTransaction, futureTimeout)

          // Try to update, which should fail
          updateAllRecords()

          ()
        }
      }

      Await.result(slowTransaction, futureTimeout) must throwA[Exception].like {
        case e =>
          RetryableException(e) must_== true
      }

    }
  }

  "A `retryableTransaction`" should {

    inExample("run the deferred actions only at the end of the transaction") in new SampleDataScope {

      var num = 0
      retryableTransaction {
        queryAllRecords

        deferToCommit {
          num = num + 1
        }

        updateAllRecords
        num must_== 0
      }

      num must_== 1

    }
  }

  "A `retryableTransaction` with deferred side effects that is retried because of a serialization error" should {

    inExample("have its side effects executed once") in new SampleDataScope {

      var num = 0

      val latch = new CountDownLatch(1)

      // First transaction
      val transaction = Future {
        retryableTransaction {
          queryAllRecords

          deferToCommit {
            // Updating this variable is the side effect we're trying to perform
            num = num + 1
          }

          latch.await()

          updateAllRecords

          // While still in the transaction, the deferred action should not be executed yet
          num must_== 0
        }
      }

      // Second transaction, this one commits first
      inTransaction {
        queryAllRecords
        updateAllRecords
      }

      // Release the first transaction
      latch.countDown()

      Await.result(transaction, futureTimeout)

      // After committing the transaction, the deferred action should be executed
      num must_== 1

    }
  }

  "If a `retryableTransaction` is retried many times because of a serialization issue and eventually fails, the side effects" should {

    inExample("not be executed") in new SampleDataScope {

      var num = 0
      // This is our slow transaction
      val slowTransaction = Future {
        retryableTransaction {
          queryAllRecords

          deferToCommit { num = num + 1 }

          // Now, in between comes a fast transaction that commits first
          val fastTransaction = Future {
            inTransaction {
              queryAllRecords
              updateAllRecords()
            }
          }
          Await.result(fastTransaction, futureTimeout)

          // Try to update, which should fail
          updateAllRecords()

          ()
        }
      }
      Await.result(slowTransaction, futureTimeout) must throwA[Exception]
      num must_== 0

    }
  }

  "If the deferred actions of a `retryableTransaction` throw an exception, the transaction" should {
    inExample("be rolled back and throw the exception") in new SampleDataScope {

      val exception = new RuntimeException("Boom!")
      currentRecordValue must_== 0
      retryableTransaction {
        updateAllRecords()

        currentRecordValue must_== 1
        deferToCommit {
          throw exception
        }
      } must throwA(exception)

      currentRecordValue must_== 0

    }
  }

  "After a `retryableTransaction` block, the ThreadLocal used to store the deferred actions" should {
    inExample("be removed") in {
      Transactions._currentSessionDeferreds.get() must beNull

      retryableTransaction {
        Transactions._currentSessionDeferreds.get() must not beNull
      }

      Transactions._currentSessionDeferreds.get() must beNull
    }
  }

  "Calling `deferToCommit` outside a `retryableTransaction` block" should {
    inExample("throw an IllegalStateException") in {
      deferToCommit() must throwA[IllegalStateException]
    }
  }

  "Deferred actions" should {
    inExample("be executed in the order they are added") in {
      var a = 0
      retryableTransaction {
        deferToCommit(a = a + 1)
        deferToCommit(a = a * 2)
      }

      // This would be '1' if the deferred actions are executed in the wrong order.
      a must_== 2
    }
  }

  // Tear down the database
  step {
    new ConnectionScope {
      inTransaction {
        TestSchema.drop
      }
    }
  }

  /*
   *
   * End of tests!
   *
   */

  object TestSchema extends Schema {
    val testRecords = table[TestRecord]("test_records");
  }

  // Read all the records. Used to construct transactions that can't be serialized with others
  def queryAllRecords = TestSchema.testRecords.toList

  // Update all the records. Used to construct transactions that can't be serialized with others
  def updateAllRecords() = update(TestSchema.testRecords)(tr => setAll(tr.testValue := tr.testValue.~ + 1))

  def readAndWaitForUpdate(countdownlatch: CountDownLatch): Unit = inTransaction {
    // Query all records
    queryAllRecords

    // Hold the transaction until the other thread arrives as well
    countdownlatch.countDown()
    countdownlatch.await()

    // Update the records
    updateAllRecords()
  }

  // Scope that has a database connection factory for Squeryl
  trait ConnectionScope extends Scope {
    Class.forName("org.postgresql.Driver")

    SessionFactory.concreteFactory = Some(() =>
      Session.create({
        val connection = java.sql.DriverManager.getConnection(System.getProperty("test.jdbc.url"), System.getProperty("test.jdbc.username"), System.getProperty("test.jdbc.password"))
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)
        // I get an exception when using setSchema, so we do this manually

        val schema = System.getProperty("test.jdbc.schema")
        connection.createStatement().execute("SET SCHEMA '" + schema + "'")
        connection
      }, new PostgreSqlAdapter))
  }

  // Scope that has a setup and teardown for sample data
  trait SampleDataScope extends ConnectionScope with After {
    cleanDb()

    val record = TestRecord(0)
    inTransaction { TestSchema.testRecords.insert(record) }

    override def after = cleanDb()

    def currentRecordValue = inTransaction {
      TestSchema.testRecords.head.testValue
    }

    def cleanDb() = inTransaction {
      TestSchema.testRecords.deleteWhere(_ => 1 === 1)
    }
  }
}

case class TestRecord(val testValue: Int)