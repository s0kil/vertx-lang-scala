package io.vertx.lang.scala

import io.vertx.core.{Future as VertxFuture, Vertx}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success}

class VertxFutureSpec extends AnyFlatSpec, Matchers:

  private def await[T](future: VertxFuture[T], timeoutMs: Long = 5000): T =
    val latch            = CountDownLatch(1)
    var result: T        = null.asInstanceOf[T]
    var error: Throwable = null
    future
      .onSuccess { t => result = t; latch.countDown() }
      .onFailure { e => error = e; latch.countDown() }
    latch.await(timeoutMs, TimeUnit.MILLISECONDS) should be(true)
    if error != null then throw error
    result

  private def awaitFailure[T](future: VertxFuture[T], timeoutMs: Long = 5000): Throwable =
    val latch            = CountDownLatch(1)
    var error: Throwable = null
    future
      .onSuccess { _ => latch.countDown() }
      .onFailure { e => error = e; latch.countDown() }
    latch.await(timeoutMs, TimeUnit.MILLISECONDS) should be(true)
    error should not be null
    error

  // ---- For-comprehension support ----

  "Vert.x Future" should "support basic for-comprehension with map/flatMap" in {
    val result = await {
      for
        a <- VertxFuture.succeededFuture(10)
        b <- VertxFuture.succeededFuture(a + 20)
      yield a + b
    }
    result should be(40)
  }

  it should "propagate failure through for-comprehension" in {
    val ex    = new RuntimeException("boom")
    val error = awaitFailure {
      for
        a <- VertxFuture.succeededFuture(10)
        b <- VertxFuture.failedFuture[Int](ex)
      yield a + b
    }
    error should be(ex)
  }

  it should "short-circuit on first failure" in {
    val ex    = new RuntimeException("first fails")
    val error = awaitFailure {
      for
        a <- VertxFuture.failedFuture[Int](ex)
        b <- VertxFuture.succeededFuture(999)
      yield a + b
    }
    error should be(ex)
  }

  it should "support chained flatMap (3+ steps)" in {
    val result = await {
      for
        a <- VertxFuture.succeededFuture(1)
        b <- VertxFuture.succeededFuture(a + 1)
        c <- VertxFuture.succeededFuture(b + 1)
        d <- VertxFuture.succeededFuture(c + 1)
      yield d
    }
    result should be(4)
  }

  // ---- withFilter ----

  it should "support guards in for-comprehensions via withFilter" in {
    val result = await {
      for
        x <- VertxFuture.succeededFuture(42)
        if x > 0
      yield x * 2
    }
    result should be(84)
  }

  it should "fail when withFilter predicate is false" in {
    val error = awaitFailure {
      for
        x <- VertxFuture.succeededFuture(-1)
        if x > 0
      yield x
    }
    error shouldBe a[NoSuchElementException]
  }

  // ---- foreach ----

  it should "support foreach for side effects" in {
    val latch    = CountDownLatch(1)
    var captured = 0
    for x <- VertxFuture.succeededFuture(99) do
      captured = x
      latch.countDown()
    latch.await(5, TimeUnit.SECONDS) should be(true)
    captured should be(99)
  }

  // ---- zip ----

  it should "zip two successful futures into a tuple" in {
    val result = await {
      VertxFuture.succeededFuture("hello").zip(VertxFuture.succeededFuture(42))
    }
    result should be(("hello", 42))
  }

  it should "fail zip if first future fails" in {
    val ex    = new RuntimeException("left fails")
    val error = awaitFailure {
      VertxFuture.failedFuture[String](ex).zip(VertxFuture.succeededFuture(42))
    }
    error should be(ex)
  }

  it should "fail zip if second future fails" in {
    val ex    = new RuntimeException("right fails")
    val error = awaitFailure {
      VertxFuture.succeededFuture("hello").zip(VertxFuture.failedFuture[Int](ex))
    }
    error should be(ex)
  }

  // ---- collect ----

  it should "collect with a defined partial function" in {
    val result = await {
      VertxFuture.succeededFuture(42).collect { case x if x > 0 => x.toString }
    }
    result should be("42")
  }

  it should "fail collect when partial function is not defined" in {
    val error = awaitFailure {
      VertxFuture.succeededFuture(-1).collect { case x if x > 0 => x.toString }
    }
    error shouldBe a[NoSuchElementException]
  }

  // ---- onCompleteTry ----

  it should "call onCompleteTry with Success on success" in {
    val latch                         = CountDownLatch(1)
    var captured: scala.util.Try[Int] = null
    VertxFuture.succeededFuture(7).onCompleteTry { t =>
      captured = t
      latch.countDown()
    }
    latch.await(5, TimeUnit.SECONDS) should be(true)
    captured should be(Success(7))
  }

  it should "call onCompleteTry with Failure on failure" in {
    val latch                         = CountDownLatch(1)
    val ex                            = new RuntimeException("fail")
    var captured: scala.util.Try[Int] = null
    VertxFuture.failedFuture[Int](ex).onCompleteTry { t =>
      captured = t
      latch.countDown()
    }
    latch.await(5, TimeUnit.SECONDS) should be(true)
    captured should be(Failure(ex))
  }

  // ---- recoverPf ----

  it should "recover from a matched exception with recoverPf" in {
    val result = await {
      VertxFuture
        .failedFuture[Int](new IllegalArgumentException("bad"))
        .recoverPf { case _: IllegalArgumentException => 0 }
    }
    result should be(0)
  }

  it should "propagate unmatched exception through recoverPf" in {
    val ex    = new RuntimeException("unmatched")
    val error = awaitFailure {
      VertxFuture
        .failedFuture[Int](ex)
        .recoverPf { case _: IllegalArgumentException => 0 }
    }
    error should be(ex)
  }

  it should "not invoke recoverPf on success" in {
    val result = await {
      VertxFuture
        .succeededFuture(42)
        .recoverPf { case _ => 0 }
    }
    result should be(42)
  }

  // ---- recoverWithPf ----

  it should "recover from a matched exception with recoverWithPf" in {
    val result = await {
      VertxFuture
        .failedFuture[Int](new IllegalArgumentException("bad"))
        .recoverWithPf { case _: IllegalArgumentException => VertxFuture.succeededFuture(99) }
    }
    result should be(99)
  }

  it should "propagate unmatched exception through recoverWithPf" in {
    val ex    = new RuntimeException("unmatched")
    val error = awaitFailure {
      VertxFuture
        .failedFuture[Int](ex)
        .recoverWithPf { case _: IllegalArgumentException => VertxFuture.succeededFuture(0) }
    }
    error should be(ex)
  }

  // ---- unit ----

  it should "convert Future[Void] to Future[Unit] with .unit" in {
    val voidFuture: VertxFuture[Void] = VertxFuture.succeededFuture[Void](null)
    val unitFuture: VertxFuture[Unit] = voidFuture.unit
    val result                        = await(unitFuture)
    result should be(())
  }

  // ---- Real Vert.x integration ----

  it should "work with actual Vert.x operations in for-comprehension" in {
    val vertx = Vertx.vertx()
    try
      val result = await {
        for
          server <- vertx
            .createHttpServer()
            .requestHandler(req => req.response().end("ok"))
            .listen(0)
          port = server.actualPort()
          _ <- server.close()
        yield port
      }
      result should be > 0
    finally vertx.close()
  }

  it should "zip real Vert.x futures" in {
    val vertx = Vertx.vertx()
    try
      val result = await {
        val f1 = vertx
          .createHttpServer()
          .requestHandler(req => req.response().end("1"))
          .listen(0)
        val f2 = vertx
          .createHttpServer()
          .requestHandler(req => req.response().end("2"))
          .listen(0)
        for (s1, s2) <- f1.zip(f2) yield
          val ports = (s1.actualPort(), s2.actualPort())
          s1.close()
          s2.close()
          ports
      }
      result._1 should be > 0
      result._2 should be > 0
      result._1 should not be result._2
    finally vertx.close()
  }
