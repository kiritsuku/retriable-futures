package retry

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.stm._

sealed trait State
case object Idle extends State
case object Retry extends State
case object Stop extends State

sealed trait Res[+A]
case object Empty extends Res[Nothing]
case class Fail(err: Throwable) extends Res[Nothing]
case class Succ[A](value: A) extends Res[A]

class RetriableFuture[A] {

  val state = Ref[State](Idle)
  val res = Ref[Res[A]](Empty)

  @volatile private var listener = () ⇒ ()

  def onSuccess[U](f: PartialFunction[A, U]): Unit = {
    listener = () ⇒  atomic { implicit txn ⇒
      res() match {
        case Succ(value) ⇒ if (f.isDefinedAt(value)) f(value)
        case _           ⇒
      }
    }
    listener()
  }

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
  def fromFuture(f: Future[A], stop: () ⇒ Unit, cont: () ⇒ Unit): RetriableFuture[A] = {
    f onSuccess {
      case value ⇒
        atomic { implicit txn ⇒
          state() = Stop
          res() = Succ(value)
          listener()
        }
        checkRetryState(stop, cont)()
    }
    f onFailure {
      case err ⇒
        atomic { implicit txn ⇒ res() = Fail(err) }
        checkRetryState(stop, cont)()
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
}
object RetriableFuture {

  def apply[A](f: ⇒ A): RetriableFuture[A] = {
    val rf = new RetriableFuture[A]
    def loop(): Unit = rf.fromFuture(Future(f), () ⇒ (), loop)
    loop()
    rf
  }

  def orElse[A](rf1: RetriableFuture[A], rf2: RetriableFuture[A]): RetriableFuture[A] = {
    fromFutOp(Seq(rf1, rf2)) {
      case Seq(rf1, rf2) ⇒
        val f1 = rf1.future
        val f2 = rf2.future
        f1 fallbackTo f2
    }
  }

  private def fromFutOp[A](fs: Seq[RetriableFuture[A]])(prod: Seq[RetriableFuture[A]] ⇒ Future[A]): RetriableFuture[A] = {
    val rf = new RetriableFuture[A]
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
