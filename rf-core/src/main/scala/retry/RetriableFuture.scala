package retry

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.stm._

/**
 * Represents if the [[RetriableFuture]] should be recomputed or not. [[Idle]]
 * means that there is a computation already going on. [[Retry]] means that
 * future has been completed, but that it should be recomputed. And [[Stop]]
 * means that no further re-computations should be done.
 */
sealed trait State
case object Idle extends State
case object Retry extends State
case object Stop extends State

/**
 * Represents the value of the [[RetriableFuture]]. [[Empty]] means no value is
 * yet computed, [[Fail]] means the computation has failed and [[Succ]] means
 * that the value could be computed successfully.
 */
sealed trait Res[+A]
case object Empty extends Res[Nothing]
final case class Fail(err: Throwable) extends Res[Nothing]
final case class Succ[A](value: A) extends Res[A]

/**
 * Contains the logic that checks if the future should be retried when it
 * failed.
 */
trait RetryStrategy {
  /**
   * Needs to be called whenever the future is retried.
   *
   * Throws an exception if a retry is not allowed.
   */
  final def triggerRetry(): Unit = {
    require(canRetry, "It is not allowed to retry the future.")
    nextRetry()
  }

  /** This is called when the future is retried. */
  protected def nextRetry(): Unit

  /** Tells whether a retry is necessary. */
  def canRetry: Boolean
}

object RetryStrategy {
  implicit class IntAsRetry(private val count: Int) extends AnyVal {
    def times: RetryStrategy = CountRetryStrategy(count)
  }
}

/**
 * Allows the future to retry for a given `duration`.
 */
final case class DurationRetryStrategy(duration: FiniteDuration) extends RetryStrategy {

  private var startTime: Long = _

  override def nextRetry(): Unit = {
    if (startTime == 0)
      startTime = System.nanoTime()
  }

  override def canRetry =
    if (startTime == 0)
      true
    else
      (System.nanoTime-startTime).nanos > duration
}

/**
 * Allows the future to retry for a given number of times.
 */
final case class CountRetryStrategy(count: Int) extends RetryStrategy {

  private var c = count

  override def nextRetry() = c -= 1
  override def canRetry = c > 0
}

trait RetriableFuture[A] {

  def strategy: RetryStrategy
  def state: Ref[State]
  def res: Ref[Res[A]]

  /** If this future succeeds `f` is called. */
  def onSuccess[U](f: PartialFunction[A, U]): Unit
  /** If this future fails `f` is called. */
  def onFailure[U](f: PartialFunction[Throwable, U]): Unit

  /**
   * Converts this [[RetriableFuture]] to a [[scala.concurrent.Future]].
   */
  def future: Future[A]

  /**
   * Converts this [[RetriableFuture]] to a [[scala.concurrent.Future]].
   * If the computation fails, it is retried as long as `strategy` allows it.
   */
  def awaitFuture: Future[A]

  def orElse(rf: RetriableFuture[A])(implicit rs: RetryStrategy): RetriableFuture[A]

  protected def fromFuture(f: Future[A], stop: () ⇒ Unit, cont: () ⇒ Unit): RetriableFuture[A]
}

final class DefaultRetriableFuture[A](override val strategy: RetryStrategy) extends RetriableFuture[A] {

  override val state = Ref[State](Idle)
  override val res = Ref[Res[A]](Empty)

  override def onSuccess[U](pf: PartialFunction[A, U]): Unit =
    awaitFuture.onSuccess(pf)

  override def onFailure[U](pf: PartialFunction[Throwable, U]): Unit =
    awaitFuture.onFailure(pf)

  /**
   * Returns either `stop` or `cont` based on the value of [[state]]. If the
   * state is [[Stop]], `stop` is returned. If it is [[Retry]], `cont` is
   * returned. In case the state is [[Idle]], the atomic block is retried.
   */
  private def checkRetryState(stop: () ⇒ Unit, cont: () ⇒ Unit): () ⇒ Unit = {
    atomic { implicit txn ⇒
      state() match {
        case Idle ⇒
          retry
        case Retry ⇒
          state() = Idle
          res() = Empty
          cont
        case Stop ⇒
          stop
      }
    }
  }

  override def fromFuture(f: Future[A], stop: () ⇒ Unit, cont: () ⇒ Unit): RetriableFuture[A] = {
    f onSuccess {
      case value ⇒
        atomic { implicit txn ⇒
          state() = Stop
          res() = Succ(value)
        }
        checkRetryState(stop, cont)()
    }
    f onFailure {
      case err ⇒
        atomic { implicit txn ⇒
          state() = Idle
          res() = Fail(err)
        }
        checkRetryState(stop, cont)()
    }
    this
  }

  override def future: Future[A] = {
    val p = Promise[A]
    atomic { implicit txn ⇒
      res() match {
        case Empty       ⇒ retry
        case Fail(err)   ⇒ p failure err
        case Succ(value) ⇒ p success value
      }
    }
    p.future
  }

  /**
   * Works similar to [[checkRetryState]]. One of the passed functions
   * `stopOnSucc`, `stopOnFail` and `cont` is executed based on the value
   * of [[res]].
   */
  private def checkRetryRes(stopOnSucc: A ⇒ Unit, stopOnFail: Throwable ⇒ Unit, cont: () ⇒ Unit): Unit = {
    atomic { implicit txn ⇒
      res() match {
        case Empty ⇒
          retry

        case Fail(err) ⇒
          if (strategy.canRetry) {
            strategy.triggerRetry()
            res() = Empty
            state() = Retry
            cont()
          }
          else
            stopOnFail(err)

        case Succ(value) ⇒
          stopOnSucc(value)
      }
    }
  }

  override def awaitFuture: Future[A] = {
    val p = Promise[A]
    def loop(): Unit = Future {
      checkRetryRes(p success, p failure, loop)
    }
    loop()
    p.future
  }

  override def orElse(rf: RetriableFuture[A])(implicit rs: RetryStrategy): RetriableFuture[A] = {
    fromFutOp(Seq(this, rf)) {
      case Seq(rf1, rf2) ⇒
        val f1 = rf1.future
        val f2 = rf2.future
        f1 fallbackTo f2
    }
  }

  private def fromFutOp[B]
      (fs: Seq[RetriableFuture[B]])
      (prod: Seq[RetriableFuture[B]] ⇒ Future[B])
      (implicit rs: RetryStrategy): RetriableFuture[B] = {
    val rf = new DefaultRetriableFuture[B](rs)
    def loop(): Unit = {
      val f = prod(fs)
      val stop = () ⇒ {
        fs foreach (rf ⇒ atomic { implicit txn ⇒ rf.state() = Stop })
      }
      val cont = () ⇒ {
        fs foreach (rf ⇒ atomic { implicit txn ⇒ rf.state() = Retry })
        loop()
      }
      rf.fromFuture(f, stop, cont)
    }
    loop()
    rf
  }
}

object RetriableFuture {

  /**
   * Creates a [[RetriableFuture]] with a given computation `f`.
   *
   * A [[RetryStrategy]] is needed, which specifies how the future should be
   * retried.
   */
  def apply[A](f: ⇒ A)(implicit rs: RetryStrategy): RetriableFuture[A] = {
    val rf = new DefaultRetriableFuture[A](rs)
    def loop(): Unit = {
      rf.fromFuture(Future(f), () ⇒ (), loop)
    }
    loop()
    rf
  }

  /**
   * Wait for the value of the future. If the computation fails, it is retries
   * as long as the [[RetryStrategy]] allows it.
   */
  def await[A](rf: RetriableFuture[A]): A = {
    Await.result(rf.awaitFuture, Duration.Inf)
  }

}
