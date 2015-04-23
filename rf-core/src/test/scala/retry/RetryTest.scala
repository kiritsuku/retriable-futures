package retry

import org.junit.Test
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import org.junit.ComparisonFailure
import scala.util.control.NoStackTrace
import org.junit.Ignore
import scala.concurrent.Future
import scala.util.control.ControlThrowable

abstract class TestHelper {

  implicit class Assert_===[A](actual: A) {
    def ===(expected: A): Unit = {
      if (exc != null)
        throw exc
      if (actual != expected)
        throw new ComparisonFailure("", expected.toString, actual.toString)
    }
  }

  /**
   * If an exception occurs in another thread, it needs to be logged here. This
   * allows us to re-throw the exception on the main thread where it is caught
   * by JUnit
   */
  @volatile private var exc: Exception = _

  def succ[A](rf: RetriableFuture[A]): Future[A] = {
    val p = Promise[A]
    rf onSuccess {
      case v =>
        try p success v catch {
          case e: Exception ⇒ exc = e
        }
    }
    p.future
  }

  def fail[A](rf: RetriableFuture[A]): Future[A] = {
    val p = Promise[A]
    rf onFailure {
      case err =>
        try p failure err catch {
          case e: Exception ⇒ exc = e
        }
    }
    p.future
  }

  def await[A](f: Future[A]): A =
    Await.result(f, Duration.Inf)

  def ex: Nothing =
    throw new TestException
}

object TestException extends RuntimeException with ControlThrowable

class RetryTest extends TestHelper {
  import RetryStrategy._

  @Test
  def no_retry() = {
    implicit val strategy = 0.times
    var i = 0
    val rf = RetriableFuture { i += 1; i }
    await(succ(rf)) === i
    i === 1
  }

  @Test
  def single_retry() = {
    implicit val strategy = 1.times
    var i = 0
    val rf = RetriableFuture { i += 1; if (i == 2) i else ex }
    await(succ(rf)) === i
    i === 2
  }

  @Test
  def multiple_retries() = {
    implicit val strategy = 10.times
    var i = 0
    val rf = RetriableFuture { i += 1; if (i == 6) i else ex }
    await(succ(rf)) === i
    i === 6
  }

  @Test
  def onSuccess_can_handle_multiple_callbacks() = {
    implicit val strategy = 10.times
    val v = 123
    val rf = RetriableFuture { v }
    1 to 10 map (_ ⇒ succ(rf)) foreach (f ⇒ await(f) === v)
  }

  @Test
  def onFailure_can_handle_multiple_callbacks() = {
    implicit val strategy = 5.times
    val rf = RetriableFuture { ex }
    1 to 10 map (_ ⇒ fail(rf)) foreach (f ⇒ await(f.failed).isInstanceOf[TestException] === true)
  }

  @Test
  def orElse_chooses_first_future_on_succ() = {
    implicit val strategy = 0.times
    val rf1 = RetriableFuture { 1 }
    val rf2 = RetriableFuture { 2 }
    val rf = rf1 orElse rf2
    await(succ(rf)) === 1
  }

  @Test
  def orElse_chooses_second_future_on_err() = {
    implicit val strategy = 0.times
    val rf1 = RetriableFuture[Int] { ex }
    val rf2 = RetriableFuture { 2 }
    val rf = rf1 orElse rf2
    await(succ(rf)) === 2
  }

  @Test
  def orElse_fails_when_both_futures_fail() = {
    implicit val strategy = 0.times
    val rf1 = RetriableFuture[Int] { ex }
    val rf2 = RetriableFuture[Int] { ex }
    val rf = rf1 orElse rf2
    await(fail(rf).failed).isInstanceOf[TestException] === true
  }
}
