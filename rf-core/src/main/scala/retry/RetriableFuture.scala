package retry

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.stm._

sealed trait State
case object Idle extends State
case object Retry extends State
case object Stop extends State

sealed trait Res[+A]
case object Empty extends Res[Nothing]
final case class Fail(err: Throwable) extends Res[Nothing]
final case class Succ[A](value: A) extends Res[A]

trait RetryStrategy {
  final def triggerRetry(): Unit = {
    require(canRetry, "It is not allowed to retry the future.")
    nextRetry()
  }
  def nextRetry(): Unit

  def canRetry: Boolean
}
object RetryStrategy {
  implicit class IntAsRetry(private val count: Int) extends AnyVal {
    def times: RetryStrategy = CountRetryStrategy(count)
  }
}

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

final case class CountRetryStrategy(count: Int) extends RetryStrategy {

  private var c = count

  override def nextRetry() = c -= 1
  override def canRetry = c > 0
}

trait RetriableFuture[A] {

  def strategy: RetryStrategy
  def state: Ref[State]
  def res: Ref[Res[A]]

  def onSuccess[U](f: PartialFunction[A, U]): Unit
  def onFailure[U](f: PartialFunction[Throwable, U]): Unit
  def fromFuture(f: Future[A], stop: () ⇒ Unit, cont: () ⇒ Unit): RetriableFuture[A]
  def future: Future[A]
  def awaitFuture: Future[A]
}

final class DefaultRetriableFuture[A](val strategy: RetryStrategy) extends RetriableFuture[A] {

  val state = Ref[State](Idle)
  val res = Ref[Res[A]](Empty)

  def onSuccess[U](pf: PartialFunction[A, U]): Unit =
    awaitFuture.onSuccess(pf)

  def onFailure[U](pf: PartialFunction[Throwable, U]): Unit =
    awaitFuture.onFailure(pf)

  private def checkRetryState(stop: () ⇒ Unit, cont: () ⇒ Unit): Unit = {
    atomic { implicit txn ⇒
      state() match {
        case Idle ⇒
          retry
        case Retry ⇒
          state() = Idle
          res() = Empty
          cont()
        case Stop ⇒
          stop()
      }
    }
  }

  def fromFuture(f: Future[A], stop: () ⇒ Unit, cont: () ⇒ Unit): RetriableFuture[A] = {
    f onSuccess {
      case value ⇒
        atomic { implicit txn ⇒
          state() = Stop
          res() = Succ(value)
        }
        checkRetryState(stop, cont)
    }
    f onFailure {
      case err ⇒
        atomic { implicit txn ⇒
          state() = Idle
          res() = Fail(err)
        }
        checkRetryState(stop, cont)
    }
    this
  }

  def future: Future[A] = {
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

  def awaitFuture: Future[A] = {
    val p = Promise[A]
    def loop(): Unit = Future {
      checkRetryRes(p success, p failure, loop)
    }
    loop
    p.future
  }
}
object RetriableFuture {

  def apply[A](f: ⇒ A)(implicit rs: RetryStrategy): RetriableFuture[A] = {
    val rf = new DefaultRetriableFuture[A](rs)
    def loop(): Unit = {
      rf.fromFuture(Future(f), () ⇒ (), loop)
    }
    loop()
    rf
  }

  def await[A](rf: RetriableFuture[A]): A = {
    Await.result(rf.awaitFuture, Duration.Inf)
  }

  def orElse[A](rf1: RetriableFuture[A], rf2: RetriableFuture[A])(implicit rs: RetryStrategy): RetriableFuture[A] = {
    fromFutOp(Seq(rf1, rf2)) {
      case Seq(rf1, rf2) ⇒
        val f1 = rf1.future
        val f2 = rf2.future
        f1 fallbackTo f2
    }
  }

  private def fromFutOp[A]
      (fs: Seq[RetriableFuture[A]])
      (prod: Seq[RetriableFuture[A]] ⇒ Future[A])
      (implicit rs: RetryStrategy): RetriableFuture[A] = {
    val rf = new DefaultRetriableFuture[A](rs)
    def loop(): Unit = {
      val f = prod(fs)
      val stop = () ⇒ {
        fs foreach (rf ⇒ atomic { implicit txn ⇒ rf.state() = Stop })
      }
      val cont = () ⇒ {
        fs foreach (rf ⇒ atomic { implicit txn ⇒ rf.state() = Retry })
        loop
      }
      rf.fromFuture(f, stop, cont)
    }
    loop()
    rf
  }

}
