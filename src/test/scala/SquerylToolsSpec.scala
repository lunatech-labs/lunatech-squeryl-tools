import org.specs2.mutable._
import org.specs2.specification.Scope
import org.squeryl.SessionFactory
import org.squeryl.Session
import org.squeryl.adapters.PostgreSqlAdapter
import org.squeryl.Schema
import org.squeryl.PrimitiveTypeMode._
import java.sql.Connection
import scala.util.control.NonFatal
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ArrayBlockingQueue
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.CountDownLatch
import scala.concurrent.duration._
import scala.concurrent.Await
import squeryltools.DbUtil._

class HelloWorldSpec extends Specification {
  sequential

  implicit val timeout = Duration(1, TimeUnit.SECONDS)

  step {
    new ConnectionScope {
      inTransaction {
        TestSchema.create

      }
    }
  }

  "2 interlocking Postgresql transactions with SERIALIZABLE isolation level" should {

    // TODO, make a matcher that can inspect cause exceptions to find the real postgres exception
    "cause one to throw an exception containing 'sqlState: 40001' in the message and abort" in new SampleDataScope {

      val latch = new CountDownLatch(2)

      val results = Future.sequence(Seq(
        Future(readAndUpdate(latch)),
        Future(readAndUpdate(latch))))

      // One of the transactions must fail
      Await.result(results, timeout) must throwAn[Exception].like {
        case e =>
          e.getMessage() must contain("sqlState: 40001")
      }

      // The value of the testrecord must be 1
      currentRecordValue must_== 1

    }
  }

  "2 interlocking Postgresql transactions with SERIALIZABLE isolation level in a retryableTransaction block" should {

    // TODO, try to confirm that it actually did something, maybe by checking the logging output somehow?
    inExample("complete successfully") in new SampleDataScope {

      val latch = new CountDownLatch(2)

      val results = Future.sequence(Seq(
        Future(retryableTransaction(readAndUpdate(latch))),
        Future(retryableTransaction(readAndUpdate(latch)))))

      Await.result(results, timeout) must have size (2)

      // The value of the testrecord must be 2
      currentRecordValue must_== 2

    }
  }

  "A slow Postgresql transaction interlocking with fast ones that commit first with SERIALIZABLE isolation level in a retryableTransaction block" should {

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
          Await.result(fastTransaction, timeout)

          // Try to update, which should fail
          updateAllRecords()

          ()
        }
      }

      Await.result(slowTransaction, timeout) must throwA[Exception].like {
        case e =>
          RetryableException(e) must_== true
      }

    }
  }

  step {
    new ConnectionScope {
      inTransaction {
        TestSchema.drop
      }
    }
  }

  object TestSchema extends Schema {
    val testRecords = table[TestRecord]("test_records");
  }

  def queryAllRecords = TestSchema.testRecords.toList
  def updateAllRecords() = update(TestSchema.testRecords)(tr => setAll(tr.testValue := tr.testValue.~ + 1))

  def readAndUpdate(countdownlatch: CountDownLatch): Unit = inTransaction {
    // Query all records
    queryAllRecords

    // Hold the transaction until the other thread arrives as well
    countdownlatch.countDown()
    countdownlatch.await()

    // Update the records
    updateAllRecords()
  }

  trait ConnectionScope extends Scope {
    Class.forName("org.postgresql.Driver")

    SessionFactory.concreteFactory = Some(() =>
      Session.create({
        val connection = java.sql.DriverManager.getConnection("jdbc:postgresql://localhost/postgres?searchpath=public", "vcs", "vcs")
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)
        // I get an exception when using setSchema, so we do this manually
        connection.createStatement().execute("SET SCHEMA 'public'")
        connection
      }, new PostgreSqlAdapter))
  }

  trait SampleDataScope extends ConnectionScope with After {
    val records = Seq(
      TestRecord(0))

    inTransaction { TestSchema.testRecords.insert(records) }

    override def after = inTransaction {
      TestSchema.testRecords.deleteWhere(_ => 1 === 1)
    }

    def currentRecordValue = inTransaction {
      TestSchema.testRecords.head.testValue
    }
  }

}

case class TestRecord(val testValue: Int)