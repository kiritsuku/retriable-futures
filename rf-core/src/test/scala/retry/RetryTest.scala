package retry

import org.junit.Test
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import org.junit.ComparisonFailure
import scala.util.control.NoStackTrace

object TestHelper {

  implicit class Assert_===[A](val actual: A) extends AnyVal {
    def ===(expected: A): Unit =
      if (actual != expected)
        throw new ComparisonFailure("", expected.toString, actual.toString)
  }

  def success[A](rf: RetriableFuture[A]): A = {
    import scala.concurrent.stm._
    atomic { implicit txn ⇒ rf.state() = Retry }

    val p = Promise[A]
    rf onSuccess {
      case v =>
        p.success(v)
    }
    Await.result(p.future, Duration.Inf)
  }
}

class TestException extends RuntimeException with NoStackTrace

class RetryTest {
  import TestHelper._

  @Test
  def no_retry() = {
    val rf = RetriableFuture(1)
    success(rf) === 1
  }

  @Test
  def single_retry() = {
    var b = false
    val rf = RetriableFuture(if (b) 1 else { b = true; throw new TestException})
    success(rf) === 1
  }

  @Test
  def multiple_retries() = {
    var i = 0
    val rf = RetriableFuture {
      i += 1
      if (i < 5)
        throw new TestException
      else
        i
    }
    success(rf) === i
  }
}
