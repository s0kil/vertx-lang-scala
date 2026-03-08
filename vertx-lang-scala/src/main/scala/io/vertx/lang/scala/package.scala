package io.vertx.lang

import concurrent.{Future as ScalaFuture, Promise as ScalaPromise}
import util.{Failure, Success, Try}
import io.vertx.lang.scala.conv.{scalaFutureToVertxFuture, vertxFutureToScalaFuture}
import io.vertx.core.{AsyncResult, DeploymentOptions, Future as VertxFuture, Handler, Promise as VertxPromise, Vertx}
import java.util.concurrent.Callable

package object scala:

  // ---------------------------------------------------------------------------
  // Vert.x Future extensions for idiomatic Scala
  // ---------------------------------------------------------------------------
  // Vert.x Future already has map/flatMap that satisfy for-comprehension
  // desugaring via SAM conversion. These extensions add the missing pieces.

  extension [T](future: VertxFuture[T])

    /** Enables pattern matching guards in for-comprehensions.
      * {{{
      * for
      *   x <- someFuture if x > 0
      * yield x
      * }}}
      */
    def withFilter(p: T => Boolean): VertxFuture[T] =
      future.map[T]((t: T) =>
        if p(t) then t
        else throw new NoSuchElementException("Vert.x Future.withFilter predicate failed")
      )

    /** Side-effectful iteration, for use with for-each:
      * {{{
      * for x <- someFuture do println(x)
      * }}}
      */
    def foreach(f: T => Unit): Unit =
      future.onSuccess(t => f(t))
      ()

    /** Composes two independent futures into a tuple.
      * {{{
      * val both: Future[(User, Config)] = getUser(id).zip(getConfig())
      * }}}
      */
    def zip[U](other: VertxFuture[U]): VertxFuture[(T, U)] =
      VertxFuture.all(future, other).map[((T, U))](_ => (future.result(), other.result()))

    /** Collects values using a partial function, failing the future if undefined.
      * {{{
      * someFuture.collect { case x if x > 0 => x * 2 }
      * }}}
      */
    def collect[U](pf: PartialFunction[T, U]): VertxFuture[U] =
      future.map[U]((t: T) =>
        if pf.isDefinedAt(t) then pf(t)
        else throw new NoSuchElementException("Vert.x Future.collect partial function not defined")
      )

    /** Scala-idiomatic completion handler using [[scala.util.Try]].
      * {{{
      * someFuture.onCompleteTry {
      *   case Success(value) => println(value)
      *   case Failure(cause) => cause.printStackTrace()
      * }
      * }}}
      */
    def onCompleteTry(handler: Try[T] => Unit): VertxFuture[T] =
      future
        .onSuccess(t => handler(Success(t)))
        .onFailure(e => handler(Failure(e)))

    /** Recovers from specific failures using a partial function. Unmatched errors propagate.
      * {{{
      * someFuture.recoverPf { case _: TimeoutException => defaultValue }
      * }}}
      */
    def recoverPf(pf: PartialFunction[Throwable, T]): VertxFuture[T] =
      future.otherwise((t: Throwable) =>
        if pf.isDefinedAt(t) then pf(t)
        else throw t
      )

    /** Recovers from specific failures with another future. Unmatched errors propagate.
      * {{{
      * someFuture.recoverWithPf { case _: TimeoutException => fallbackFuture }
      * }}}
      */
    def recoverWithPf(pf: PartialFunction[Throwable, VertxFuture[T]]): VertxFuture[T] =
      future.recover((t: Throwable) =>
        if pf.isDefinedAt(t) then pf(t)
        else VertxFuture.failedFuture(t)
      )

    /** Converts to a [[scala.concurrent.Future]] for interop with Scala ecosystem libraries. */
    def asScala: ScalaFuture[T] = vertxFutureToScalaFuture(future)

  /** Converts a [[VertxFuture]][Void] to [[VertxFuture]][Unit] for ergonomic Scala usage. */
  extension (future: VertxFuture[Void]) def unit: VertxFuture[Unit] = future.map[Unit]((_: Void) => ())

  // ---------------------------------------------------------------------------
  // Scala Future/Promise ↔ Vert.x conversions (for ecosystem interop)
  // ---------------------------------------------------------------------------

  extension [T](scalaFuture: ScalaFuture[T]) def asVertx: VertxFuture[T] = scalaFutureToVertxFuture(scalaFuture)

  extension [T](vertxPromise: VertxPromise[T])
    def asScala: ScalaPromise[T] =
      val scalaPromise = ScalaPromise[T]()
      vertxPromise.future
        .onSuccess(scalaPromise.success(_))
        .onFailure(scalaPromise.failure(_))
      scalaPromise

  extension [T](scalaPromise: ScalaPromise[T])
    def asVertx: VertxPromise[T] =
      val vertxPromise = VertxPromise.promise[T]()
      scalaPromise.future.asVertx
        .onSuccess(vertxPromise.complete(_))
        .onFailure(vertxPromise.fail(_))
      vertxPromise

  /** Turns a Vert.x callback function into a [[ScalaFuture]].
    * @param f
    *   the callback function; if there are more parameters than just `handler`, you may apply it partially
    */
  def handleInFuture[T](f: Handler[AsyncResult[T]] => Unit): ScalaFuture[T] =
    val promise = ScalaPromise[T]()
    f(ar => if ar.succeeded then promise.success(ar.result) else promise.failure(ar.cause))
    promise.future

  // ---------------------------------------------------------------------------
  // Vertx instance extensions
  // ---------------------------------------------------------------------------

  extension (asJava: Vertx)

    /** Like [[deployVerticle]] but returns a Scala Future instead of taking an AsyncResultHandler.
      */
    def deployVerticle(verticle: ScalaVerticle): ScalaFuture[String] =
      asJava.deployVerticle(verticle.asJava).asScala

    /** Like [[deployVerticle]] but returns a Scala Future instead of taking an AsyncResultHandler.
      */
    def deployVerticle(verticle: ScalaVerticle, options: DeploymentOptions): ScalaFuture[String] =
      asJava.deployVerticle(verticle.asJava, options).asScala

    /** Safely execute some blocking code.
      *
      * Executes the blocking code in the handler `blockingCodeHandler` using a thread from the worker pool.
      *
      * When the code is complete the returned Future will be completed with the result.
      *
      * @param blockingFunction
      *   function containing blocking code
      * @param ordered
      *   if true then if executeBlocking is called several times on the same context, the executions for that context
      *   will be executed serially, not in parallel. if false then they will be no ordering guarantees
      * @return
      *   a Future representing the result of the blocking operation
      */
    def executeBlockingScala[T](blockingFunction: () => T, ordered: Boolean = true): concurrent.Future[T] =
      val c: Callable[T] = () => blockingFunction()
      asJava.executeBlocking[T](c, ordered).asScala

    /** Set a default exception handler for [[io.vertx.core.Context]], set on [[io.vertx.core.Context#exceptionHandler]]
      * at creation. * @param handler the exception handler
      *
      * @return
      *   a reference to this, so the API can be used fluently
      */
    def exceptionHandler(handler: Option[Throwable => Unit]): Vertx =
      asJava.exceptionHandler(handler.map(hdlr => hdlr.asInstanceOf[Handler[java.lang.Throwable]]).orNull)

    /** Like close from [[io.vertx.core.Vertx]] but returns a Scala Future instead of taking an AsyncResultHandler.
      */
    def closeFuture(): ScalaFuture[Unit] = asJava.close
      .map((_: Void) => ())
      .asScala
