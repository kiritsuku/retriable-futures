package retry

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration

sealed trait State
case object Idle extends State
case object Retry extends State
case object Stop extends State

sealed trait Res[+A]
case object Empty extends Res[Nothing]
case class Fail(err: Throwable) extends Res[Nothing]
case class Succ[A](value: A) extends Res[A]

trait RetriableFuture[A] {
  def comp: () ⇒ A

  @volatile var state: State = Retry
  @volatile var res: Res[A] = Empty

  @volatile private var listener = () ⇒ ()

  def onSuccess[U](f: PartialFunction[A, U]): Unit = {
    listener = () ⇒ res match {
      case Succ(value) ⇒ if (f.isDefinedAt(value)) f(value)
      case _ ⇒
    }
    listener()
  }

  private def retry(): Unit = {
    val f = Future(comp())
    f onSuccess {
      case value ⇒
        state = Stop
        res = Succ(value)

        listener()
    }
    f onFailure {
      case err ⇒
        res = Fail(err)
        state match {
          case Idle ⇒ ???
          case Retry ⇒ retry()
          case Stop ⇒
        }
    }
  }
}
object RetriableFuture {
  def apply[A](f: ⇒ A): RetriableFuture[A] = {
    val rf = new RetriableFuture[A] {
      override val comp = () ⇒ f
    }
    rf.retry()
    rf
  }
}
object AtomicRefUtils {
  implicit class RichAtomicRef[A](private val ref: AtomicReference[A]) extends AnyVal {
    def update(a: A): Unit =
      ref.set(a)
    def apply(): A =
      ref.get
  }
}
