package retry

import retry.util.FutureUtils._
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

  private type Listener = Vector[Res[A] ⇒ Boolean]
  private object Listener { def apply(xs: Res[A] ⇒ Boolean*): Listener = Vector(xs: _*) }

  val state = Ref[State](Idle)
  val res = Ref[Res[A]](Empty)
  val listeners = Ref[Listener](Listener())

  def onSuccess[U](pf: PartialFunction[A, U]): Unit =
    awaitFuture.onSuccess(pf)
  // TODO find out how atomic works. We need to sync onSuccess/onFailure/notifyListeners here
  /*def onSuccess[U](f: PartialFunction[A, U]): Unit = atomic { implicit txn ⇒
    listeners() = listeners() :+ ((_: Res[A]) match {
      case Succ(value) if f isDefinedAt value ⇒ f(value); true
      case _                                  ⇒ false
    })
    notifyListeners()
  }*/

  def onFailure[U](pf: PartialFunction[Throwable, U]): Unit =
    awaitFuture.onFailure(pf)
  /*def onFailure[U](pf: PartialFunction[Throwable, U]): Unit = atomic { implicit txn ⇒
    listeners() = listeners() :+ ((_: Res[A]) match {
      case Fail(err) if pf isDefinedAt err ⇒ pf(err); true
      case _                               ⇒ false
    })
    notifyListeners()
  }*/

  private def checkRetryState(from: String, stop: () ⇒ Unit, cont: () ⇒ Unit): () ⇒ Unit = {
    atomic { implicit txn ⇒
      state() match {
        case Idle ⇒
          println(s"checkRetryState#Idle")
          retry
        case Retry ⇒
          println("checkRetryState#Retry")
          state() = Idle
          res() = Empty
          strategy.triggerRetry()
          cont
        case Stop ⇒
          println("checkRetryState#Stop")
          stop
      }
    }
  }

  def fromFuture(f: Future[A], stop: () ⇒ Unit, cont: () ⇒ Unit): RetriableFuture[A] = {
    println("fromFuture")
    f onSuccess {
      case value ⇒
        println("fromFuture#succ")
        atomic { implicit txn ⇒
          state() = Stop
          res() = Succ(value)
          notifyListeners()
          checkRetryState("succ", stop, cont)()
        }
    }
    f onFailure {
      case err ⇒
        println("fromFuture#fail")
        atomic { implicit txn ⇒
          println("fromFuture#fail#1")
          res() = Fail(err)
          println("fromFuture#fail#2")
          notifyListeners()
          checkRetryState("fail", stop, cont)()
        }
        println("fromFuture#fail#3")
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

  def awaitFuture: Future[A] = {
    println("awaitFuture")
    val p = Promise[A]
    def loop(): Unit = Future(atomic { implicit txn ⇒
      res() match {
        case Empty ⇒
          println("awaitFuture#loop#retry")
          retry

        case Fail(err) ⇒
          println("awaitFuture#loop#fail")
          if (strategy.canRetry) {
            println("awaitFuture#loop#retry")
            state() = Retry
          }
          else {
            println("awaitFuture#loop#stop")
            p failure err
            state() = Stop
          }
          loop
//          checkRetryState(() ⇒ (println("awaitFuture#loop#stop")), loop)()

        case Succ(value) ⇒
          println("awaitFuture#loop#succ")
          state() = Stop
          p success value
      }
    })
    loop
    p.future
  }

  private def notifyListeners()(implicit txn: InTxn) = {
    val r = res()
    val listeners2 = (listeners() foldLeft Listener()) {
      case (listeners, l) ⇒
        if (l(r)) listeners else listeners :+ l
    }
    listeners() = listeners2
  }
}
object RetriableFuture {

  def apply[A](f: ⇒ A)(implicit rs: RetryStrategy): RetriableFuture[A] = {
    val rf = new DefaultRetriableFuture[A](rs)
    def loop(): Unit = {
      println("apply#loop")
      rf.fromFuture(Future(f), () ⇒ (println("apply#stop")), loop)
    }
    loop()
    rf
  }

  def await[A](rf: RetriableFuture[A]): A = {
    Await.result(rf.awaitFuture, Duration.Inf)
  }
  /*
  def orElse[A](rf1: RetriableFuture[A], rf2: RetriableFuture[A]): RetriableFuture[A] = {
    fromFutOp(Seq(rf1, rf2)) {
      case Seq(rf1, rf2) ⇒
        val f1 = rf1.future
        val f2 = rf2.future
        f1 fallbackTo f2
    }
  }

  private def fromFutOp[A](fs: Seq[RetriableFuture[A]])(prod: Seq[RetriableFuture[A]] ⇒ Future[A]): RetriableFuture[A] = {
    val rf = new DefaultRetriableFuture[A]
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
  */

}
