squeryltools
===

squerytools is a tiny library that provides retryable transactions for Postgresql and Squeryl.

When working with relational databases, the serializable isolation level is sometimes a good choice, because the database guarantees that if a transaction works well in isolation, it will also work well if executed concurrently with other transactions. It can guarantee this by _serializing_ transactions: the effect of transactions _A_ and _B_ executed concurrently is exactly the same as running _A_ and then _B_ sequentially or _B_ and then _A_ sequentially.

This simplified reasoning model comes at the cost of potential serialization errors. If two transactions read the same value, and they then both update that value, the DB is unable to serialize those transactions. The second transaction to write the value will fail with a serialization exception.

Often, if the second transaction is retried, it will succeed.

This library helps with that, by providing a method `retryableTransaction` which takes a thunk as parameter and will retry it when it throws an exception that indicats a serialization failure.

A problem with retrying an transaction is that sometimes a transaction has side effects that should only be executed once, if the transaction succeeds. For example, sending out an email should not happen multiple times if a transaction is retried multiple times.

This library provides a `deferToCommit` method that takes a thunk and only executes it once at the end of the transaction, even if the transaction needs to be retried multiple times before it succeeds.

Testing
---

Testing this library requires a running Postgresql database and a user that has permissions to create a table.

Copy the following code into a file `db.sbt` and update to your settings:

    testOptions in Test += Tests.Setup { () => 
      System.setProperty("test.jdbc.url", "jdbc:postgresql://localhost/postgres?searchpath=public")
      System.setProperty("test.jdbc.username", "foo")
      System.setProperty("test.jdbc.password", "bar")
      System.setProperty("test.jdbc.schema", "public")
    }
    
