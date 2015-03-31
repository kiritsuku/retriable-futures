package retry

import org.junit.Test
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import org.junit.ComparisonFailure
import scala.util.control.NoStackTrace
import org.junit.Ignore
import scala.concurrent.Future

object TestHelper {

  implicit class Assert_===[A](val actual: A) extends AnyVal {
    def ===(expected: A): Unit =
      if (actual != expected)
        throw new ComparisonFailure("", expected.toString, actual.toString)
  }

  def fut[A](rf: RetriableFuture[A]): Future[A] = {
    val p = Promise[A]
    rf onSuccess {
      case v =>
        p success v
    }
    p.future
  }

  def await[A](f: Future[A]): A =
    Await.result(f, Duration.Inf)

  def ex: Nothing =
    throw new TestException
}

class TestException extends RuntimeException with NoStackTrace

class RetryTest {
  import TestHelper._

  @Test
  def no_retry() = {
    var i = 0
    val rf = RetriableFuture { i += 1; i }
    await(fut(rf)) === i
    i === 1
  }

  @Test @Ignore
  def single_retry() = {
    var i = 0
    val rf = RetriableFuture { i += 1; if (i == 2) i else ex }
    await(fut(rf)) === i
    i === 1
  }

  @Test @Ignore
  def multiple_retries() = {
    var i = 0
    val rf = RetriableFuture { i += 1; if (i == 6) i else ex }
    await(fut(rf)) === i
    i === 5
  }

  @Test
  def onSuccess_registers_callback() = {
    val v = 123
    val rf = RetriableFuture { v }
    1 to 10 map (_ ⇒ fut(rf)) foreach (f ⇒ await(f) === v)
  }
}
