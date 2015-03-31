package retry

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.control.NoStackTrace
import scala.concurrent.stm._

sealed trait State
case object Idle extends State
case object Retry extends State
case object Stop extends State

sealed trait Res[+A]
case object Empty extends Res[Nothing]
case class Fail(err: Throwable) extends Res[Nothing]
case class Succ[A](value: A) extends Res[A]

final class ForceRetry extends RuntimeException with NoStackTrace

trait RetriableFuture[A] {
  def comp: () ⇒ A

  val state = Ref[State](Idle)
  val res = Ref[Res[A]](Empty)

  @volatile private var listener = () ⇒ ()

  def onSuccess[U](f: PartialFunction[A, U]): Unit = {
    listener = () ⇒  atomic { implicit txn ⇒
      res() match {
        case Succ(value) ⇒ if (f.isDefinedAt(value)) f(value)
        case _ ⇒
      }
    }
    listener()
  }

  /*
  def orElse[B >: A](rf: RetriableFuture[B]): RetriableFuture[B] = {
    val f1 = RetriableFuture.asFuture(this)
    val f2 = RetriableFuture.asFuture(rf)
    val f = f1 fallbackTo f2

    ???
  }
  */

  /*private def retryComp2(fs: Seq[RetriableFuture[A]], prod: Seq[RetriableFuture[A]] ⇒ Future[A]): Unit = {
    val f = prod(fs)

    f onSuccess {
      case value ⇒
        atomic { implicit txn ⇒ state() = Stop }
        fs foreach (_.state = Stop)
        res = Succ(value)
    }
    f onFailure {
      case err ⇒
        res = Fail(err)
        state match {
          case Idle ⇒ ???
          case Retry ⇒
            fs foreach (_.state = Retry)
            retryComp2(fs, prod)
          case Stop ⇒
            fs foreach (_.state = Stop)
        }
    }
  }*/

  private def retryComp(): Unit = {
    val f = Future(comp())
    f onSuccess {
      case value ⇒
        atomic { implicit txn ⇒ state() = Stop }
        atomic { implicit txn ⇒ res() = Succ(value) }
        listener()
    }
    f onFailure {
      case err ⇒
        atomic { implicit txn ⇒ res() = Fail(err) }

        // leave atomic block as early as possible
        var c = () ⇒ ()
        atomic { implicit txn ⇒
          state() match {
            case Idle ⇒ retry
            case Retry ⇒
              atomic { implicit txn ⇒ state() = Idle }
              c = retryComp
            case Stop ⇒
          }
        }
        c()
    }
  }
}
object RetriableFuture {
  /*def asFuture[A](rf: RetriableFuture[A]): Future[A] = {
    val p = Promise[A]
    RetriableFuture {
      rf.res match {
        case Empty ⇒ forceRetry
        case Fail(err) ⇒ p.failure(err)
        case Succ(value) ⇒ p.success(value)
      }
    }
    p.future
  }

  def fromFuture[A](f: Future[A]): RetriableFuture[A] = {
    val rf = new RetriableFuture[A] {
      override val comp = () ⇒ if (!f.isCompleted) forceRetry
    }

    f onSuccess {
      case value ⇒

    }
    ???
  }*/

  def apply[A](f: ⇒ A): RetriableFuture[A] = {
    val rf = new RetriableFuture[A] {
      override val comp = () ⇒ f
    }
    rf.retryComp()
    rf
  }

  private def forceRetry =
    throw new ForceRetry

}
object AtomicRefUtils {
  implicit class RichAtomicRef[A](private val ref: AtomicReference[A]) extends AnyVal {
    def update(a: A): Unit =
      ref.set(a)
    def apply(): A =
      ref.get
  }
}
