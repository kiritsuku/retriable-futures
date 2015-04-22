package benchmark

import java.awt.Color

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.jfree.chart.plot.XYPlot
import org.scalameter.Aggregator
import org.scalameter.CurveData
import org.scalameter.Executor
import org.scalameter.Gen
import org.scalameter.History
import org.scalameter.Measurer
import org.scalameter.PerformanceTest
import org.scalameter.Persistor
import org.scalameter.execution.SeparateJvmsExecutor
import org.scalameter.reporting.ChartReporter
import org.scalameter.reporting.ChartReporter.ChartFactory

import retry._
import scalax.chart.Chart

object Test extends PerformanceTest {

  // This exists only because we want to set our own label for the y axis.
  final class XYLine extends ChartFactory.XYLine {
    override def createChart(scopename: String, cs: Seq[CurveData], histories: Seq[History], colors: Seq[Color] = Seq()): Chart = {
      val c = super.createChart(scopename, cs, histories, colors)
      c.plot.asInstanceOf[XYPlot].getRangeAxis.setLabel("time in ms")
      c
    }
  }

  lazy val executor = SeparateJvmsExecutor(
    new Executor.Warmer.Default,
    Aggregator.min,
    new Measurer.Default
  )
  lazy val persistor = Persistor.None
  lazy val reporter = ChartReporter(new XYLine())

  val sizes = Gen.range("# of Futures")(50, 500, 50)

  performance of "SimpleCombinator" in {
    measure method "orElse" in {
      using(sizes) curve "Retriable" in { size ⇒
        val f = retriable.create(size)
        f.onSuccess { case _ ⇒ () }
      }
      using(sizes) curve "Standard" in { size ⇒
        val f = standard.create(size)
        f.onSuccess { case _ ⇒ () }
      }
    }
  }

  object standard {
    def create(n: Int) = {
      require(n > 1)
      val f = 1 to n-1 map (_ ⇒ fail()) reduceLeft (_ fallbackTo _)
      f fallbackTo succ()
    }

    def fail() =
      Future[Int] { throw new TestException }

    def succ() =
      Future { 1 }
  }

  object retriable {
    import RetryStrategy._

    def create(n: Int) = {
      require(n > 1)
      implicit val strategy = 0.times
      val f = 1 to n-1 map (_ ⇒ fail) reduceLeft (_ orElse _)
      f orElse succ
    }

    def fail(implicit strategy: RetryStrategy) =
      RetriableFuture[Int] { throw new TestException }

    def succ(implicit strategy: RetryStrategy) =
      RetriableFuture { 1 }
  }
}
