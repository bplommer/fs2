/*
 * Copyright (c) 2013 Functional Streams for Scala
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2
package io
package tcp

import scala.concurrent.duration._

import java.net.{InetSocketAddress, SocketAddress, StandardSocketOptions}
import java.nio.{Buffer, ByteBuffer}
import java.nio.channels.{
  AsynchronousCloseException,
  AsynchronousServerSocketChannel,
  AsynchronousSocketChannel,
  CompletionHandler
}
import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.spi.AsynchronousChannelProvider
import java.util.concurrent.{ThreadFactory, TimeUnit}

import cats.syntax.all._
import cats.effect.{Async, Blocker, Concurrent, ContextShift, Resource, Sync}
import cats.effect.concurrent.{Ref, Semaphore}

import fs2.internal.ThreadFactories

/**
  * Resource that provides the ability to open client and server TCP sockets that all share
  * an underlying non-blocking channel group.
  */
final class SocketGroup(channelGroup: AsynchronousChannelGroup, blocker: Blocker) {

  /**
    * Opens a connection to the specified server represented as a [[Socket]].
    * The connection is closed when the resource is released.
    *
    * @param to                   address of remote server
    * @param reuseAddress         whether address may be reused (see `java.net.StandardSocketOptions.SO_REUSEADDR`)
    * @param sendBufferSize       size of send buffer  (see `java.net.StandardSocketOptions.SO_SNDBUF`)
    * @param receiveBufferSize    size of receive buffer  (see `java.net.StandardSocketOptions.SO_RCVBUF`)
    * @param keepAlive            whether keep-alive on tcp is used (see `java.net.StandardSocketOptions.SO_KEEPALIVE`)
    * @param noDelay              whether tcp no-delay flag is set  (see `java.net.StandardSocketOptions.TCP_NODELAY`)
    */
  def client[F[_]](
      to: InetSocketAddress,
      reuseAddress: Boolean = true,
      sendBufferSize: Int = 256 * 1024,
      receiveBufferSize: Int = 256 * 1024,
      keepAlive: Boolean = false,
      noDelay: Boolean = false,
      additionalSocketOptions: List[SocketOptionMapping[_]] = List.empty
  )(implicit F: Concurrent[F], CS: ContextShift[F]): Resource[F, Socket[F]] =
    clientIn[F, F](
      to,
      reuseAddress,
      sendBufferSize,
      receiveBufferSize,
      keepAlive,
      noDelay,
      additionalSocketOptions
    )

  def clientIn[F[_], G[_]](
      to: InetSocketAddress,
      reuseAddress: Boolean = true,
      sendBufferSize: Int = 256 * 1024,
      receiveBufferSize: Int = 256 * 1024,
      keepAlive: Boolean = false,
      noDelay: Boolean = false,
      additionalSocketOptions: List[SocketOptionMapping[_]] = List.empty
  )(implicit
      F: Async[F],
      csF: ContextShift[F],
      G: Concurrent[G],
      csG: ContextShift[G]
  ): Resource[F, Socket[G]] = {
    def setup: F[AsynchronousSocketChannel] =
      blocker.delay[F, AsynchronousSocketChannel] {
        val ch =
          AsynchronousChannelProvider.provider().openAsynchronousSocketChannel(channelGroup)
        ch.setOption[java.lang.Boolean](StandardSocketOptions.SO_REUSEADDR, reuseAddress)
        ch.setOption[Integer](StandardSocketOptions.SO_SNDBUF, sendBufferSize)
        ch.setOption[Integer](StandardSocketOptions.SO_RCVBUF, receiveBufferSize)
        ch.setOption[java.lang.Boolean](StandardSocketOptions.SO_KEEPALIVE, keepAlive)
        ch.setOption[java.lang.Boolean](StandardSocketOptions.TCP_NODELAY, noDelay)
        additionalSocketOptions.foreach { case SocketOptionMapping(option, value) =>
          ch.setOption(option, value)
        }
        ch
      }

    def connect(ch: AsynchronousSocketChannel): F[AsynchronousSocketChannel] =
      asyncYield[F, AsynchronousSocketChannel] { cb =>
        ch.connect(
          to,
          null,
          new CompletionHandler[Void, Void] {
            def completed(result: Void, attachment: Void): Unit =
              cb(Right(ch))
            def failed(rsn: Throwable, attachment: Void): Unit =
              cb(Left(rsn))
          }
        )
      }

    Resource.liftF(setup.flatMap(connect)).flatMap(in[F, G](_))
  }

  /**
    * Stream that binds to the specified address and provides a connection for,
    * represented as a [[Socket]], for each client that connects to the bound address.
    *
    * Returns a stream of stream of sockets.
    *
    * The outer stream scopes the lifetime of the server socket.
    * When the outer stream terminates, all open connections will terminate as well.
    * The outer stream emits an element (an inner stream) for each client connection.
    *
    * Each inner stream represents an individual connection, and as such, is a stream
    * that emits a single socket. Failures that occur in an inner stream do *NOT* cause
    * the outer stream to fail.
    *
    * @param address            address to accept connections from
    * @param maxQueued          number of queued requests before they will become rejected by server
    *                           (supply <= 0 for unbounded)
    * @param reuseAddress       whether address may be reused (see `java.net.StandardSocketOptions.SO_REUSEADDR`)
    * @param receiveBufferSize  size of receive buffer (see `java.net.StandardSocketOptions.SO_RCVBUF`)
    */
  def server[F[_]](
      address: InetSocketAddress,
      maxQueued: Int = 0,
      reuseAddress: Boolean = true,
      receiveBufferSize: Int = 256 * 1024,
      additionalSocketOptions: List[SocketOptionMapping[_]] = List.empty
  )(implicit
      F: Concurrent[F],
      CS: ContextShift[F]
  ): Stream[F, Resource[F, Socket[F]]] = {
    val _ = maxQueued // TODO delete maxQueued in 3.0
    serverIn[F, F, F](address, reuseAddress, receiveBufferSize, additionalSocketOptions)
  }

  def serverIn[F[_], F0[_], G[_]](
      address: InetSocketAddress,
      reuseAddress: Boolean = true,
      receiveBufferSize: Int = 256 * 1024,
      additionalSocketOptions: List[SocketOptionMapping[_]] = List.empty
  )(implicit
      F: Async[F],
      cs: ContextShift[F],
      F0: Async[F0],
      cs0: ContextShift[F0],
      G: Concurrent[G],
      csG: ContextShift[G]
  ): Stream[F, Resource[F0, Socket[G]]] =
    Stream
      .resource(
        serverResourceIn[F, F, F0, G](
          address,
          reuseAddress,
          receiveBufferSize,
          additionalSocketOptions
        )
      )
      .flatMap { case (_, clients) => clients }

  @deprecated("Use serverResource instead", "2.2.0")
  def serverWithLocalAddress[F[_]](
      address: InetSocketAddress,
      maxQueued: Int = 0,
      reuseAddress: Boolean = true,
      receiveBufferSize: Int = 256 * 1024,
      additionalSocketOptions: List[SocketOptionMapping[_]] = List.empty
  )(implicit
      F: Concurrent[F],
      CS: ContextShift[F]
  ): Stream[F, Either[InetSocketAddress, Resource[F, Socket[F]]]] = {
    val _ = maxQueued
    Stream
      .resource(serverResource(address, reuseAddress, receiveBufferSize, additionalSocketOptions))
      .flatMap { case (localAddress, clients) =>
        Stream(Left(localAddress)) ++ clients.map(Right(_))
      }
  }

  /**
    * Like [[server]] but provides the `InetSocketAddress` of the bound server socket before providing accepted sockets.
    * The inner stream emits one socket for each client that connects to the server.
    */
  def serverResource[F[_]](
      address: InetSocketAddress,
      reuseAddress: Boolean = true,
      receiveBufferSize: Int = 256 * 1024,
      additionalSocketOptions: List[SocketOptionMapping[_]] = List.empty
  )(implicit
      F: Concurrent[F],
      CS: ContextShift[F]
  ): Resource[F, (InetSocketAddress, Stream[F, Resource[F, Socket[F]]])] =
    serverResourceIn[F, F, F, F](address, reuseAddress, receiveBufferSize, additionalSocketOptions)

  def serverResourceIn[F[_], F0[_], F1[_], G[_]](
      address: InetSocketAddress,
      reuseAddress: Boolean = true,
      receiveBufferSize: Int = 256 * 1024,
      additionalSocketOptions: List[SocketOptionMapping[_]] = List.empty
  )(implicit
      F: Sync[F],
      cs: ContextShift[F],
      F0: Async[F0],
      cs0: ContextShift[F0],
      F1: Async[F1],
      cs1: ContextShift[F1],
      G: Concurrent[G],
      csG: ContextShift[G]
  ): Resource[F, (InetSocketAddress, Stream[F0, Resource[F1, Socket[G]]])] = {

    val setup: F[AsynchronousServerSocketChannel] =
      blocker.delay[F, AsynchronousServerSocketChannel] {
        val ch = AsynchronousChannelProvider
          .provider()
          .openAsynchronousServerSocketChannel(channelGroup)
        ch.setOption[java.lang.Boolean](StandardSocketOptions.SO_REUSEADDR, reuseAddress)
        ch.setOption[Integer](StandardSocketOptions.SO_RCVBUF, receiveBufferSize)
        additionalSocketOptions.foreach { case SocketOptionMapping(option, value) =>
          ch.setOption(option, value)
        }
        ch.bind(address)
        ch
      }

    def cleanup(sch: AsynchronousServerSocketChannel): F[Unit] =
      blocker.delay[F, Unit](if (sch.isOpen) sch.close())

    def acceptIncoming(
        sch: AsynchronousServerSocketChannel
    ): Stream[F0, Resource[F1, Socket[G]]] = {
      def go: Stream[F0, Resource[F1, Socket[G]]] = {
        def acceptChannel: F0[AsynchronousSocketChannel] =
          asyncYield[F0, AsynchronousSocketChannel] { cb =>
            sch.accept(
              null,
              new CompletionHandler[AsynchronousSocketChannel, Void] {
                def completed(ch: AsynchronousSocketChannel, attachment: Void): Unit =
                  cb(Right(ch))
                def failed(rsn: Throwable, attachment: Void): Unit =
                  cb(Left(rsn))
              }
            )
          }

        Stream.eval(acceptChannel.attempt).flatMap {
          case Left(_)         => Stream.empty[F0]
          case Right(accepted) => Stream.emit(in[F1, G](accepted))
        } ++ go
      }

      go.handleErrorWith {
        case err: AsynchronousCloseException =>
          Stream.eval(blocker.delay[F0, Boolean](sch.isOpen)).flatMap { isOpen =>
            if (isOpen) Stream.raiseError[F0](err)
            else Stream.empty
          }
        case err => Stream.raiseError[F0](err)
      }
    }

    Resource.make[F, AsynchronousServerSocketChannel](setup)(cleanup).map { sch =>
      val localAddress = sch.getLocalAddress.asInstanceOf[InetSocketAddress]
      (localAddress, acceptIncoming(sch))
    }
  }

  private def in[F[_], G[_]](
      ch: AsynchronousSocketChannel
  )(implicit
      F: Sync[F],
      cs: ContextShift[F],
      G: Concurrent[G],
      csG: ContextShift[G]
  ): Resource[F, Socket[G]] = {
    val socket = (
      Semaphore.in[F, G](1),
      Semaphore.in[F, G](1),
      Ref.in[F, G, ByteBuffer](ByteBuffer.allocate(0))
    ).mapN { (readSemaphore, writeSemaphore, bufferRef) =>
      // Reads data to remaining capacity of supplied ByteBuffer
      // Also measures time the read took returning this as tuple
      // of (bytes_read, read_duration)
      def readChunk(buff: ByteBuffer, timeoutMs: Long): G[(Int, Long)] =
        asyncYield[G, (Int, Long)] { cb =>
          val started = System.currentTimeMillis()
          ch.read(
            buff,
            timeoutMs,
            TimeUnit.MILLISECONDS,
            (),
            new CompletionHandler[Integer, Unit] {
              def completed(result: Integer, attachment: Unit): Unit = {
                val took = System.currentTimeMillis() - started
                cb(Right((result, took)))
              }
              def failed(err: Throwable, attachment: Unit): Unit =
                cb(Left(err))
            }
          )
        }

      // gets buffer of desired capacity, ready for the first read operation
      // If the buffer does not have desired capacity it is resized (recreated)
      // buffer is also reset to be ready to be written into.
      def getBufferOf(sz: Int): G[ByteBuffer] =
        bufferRef.get.flatMap { buff =>
          if (buff.capacity() < sz)
            G.delay(ByteBuffer.allocate(sz)).flatTap(bufferRef.set)
          else
            G.delay {
              (buff: Buffer).clear()
              (buff: Buffer).limit(sz)
              buff
            }
        }

      // When the read operation is done, this will read up to buffer's position bytes from the buffer
      // this expects the buffer's position to be at bytes read + 1
      def releaseBuffer(buff: ByteBuffer): G[Chunk[Byte]] =
        G.delay {
          val read = buff.position()
          val result =
            if (read == 0) Chunk.bytes(Array.empty)
            else {
              val dest = new Array[Byte](read)
              (buff: Buffer).flip()
              buff.get(dest)
              Chunk.bytes(dest)
            }
          (buff: Buffer).clear()
          result
        }

      def read0(max: Int, timeout: Option[FiniteDuration]): G[Option[Chunk[Byte]]] =
        readSemaphore.withPermit {
          getBufferOf(max).flatMap { buff =>
            readChunk(buff, timeout.map(_.toMillis).getOrElse(0L)).flatMap { case (read, _) =>
              if (read < 0) G.pure(None)
              else releaseBuffer(buff).map(Some(_))
            }
          }
        }

      def readN0(max: Int, timeout: Option[FiniteDuration]): G[Option[Chunk[Byte]]] =
        readSemaphore.withPermit {
          getBufferOf(max).flatMap { buff =>
            def go(timeoutMs: Long): G[Option[Chunk[Byte]]] =
              readChunk(buff, timeoutMs).flatMap { case (readBytes, took) =>
                if (readBytes < 0 || buff.position() >= max)
                  // read is done
                  releaseBuffer(buff).map(Some(_))
                else go((timeoutMs - took).max(0))
              }

            go(timeout.map(_.toMillis).getOrElse(0L))
          }
        }

      def write0(bytes: Chunk[Byte], timeout: Option[FiniteDuration]): G[Unit] = {
        def go(buff: ByteBuffer, remains: Long): G[Unit] =
          asyncYield[G, Option[Long]] { cb =>
            val start = System.currentTimeMillis()
            ch.write(
              buff,
              remains,
              TimeUnit.MILLISECONDS,
              (),
              new CompletionHandler[Integer, Unit] {
                def completed(result: Integer, attachment: Unit): Unit =
                  cb(
                    Right(
                      if (buff.remaining() <= 0) None
                      else Some(System.currentTimeMillis() - start)
                    )
                  )
                def failed(err: Throwable, attachment: Unit): Unit =
                  cb(Left(err))
              }
            )
          }.flatMap {
            case None       => G.unit
            case Some(took) => go(buff, (remains - took).max(0))
          }
        writeSemaphore.withPermit {
          go(bytes.toByteBuffer, timeout.map(_.toMillis).getOrElse(0L))
        }
      }

      ///////////////////////////////////
      ///////////////////////////////////

      new Socket[G] {
        def readN(numBytes: Int, timeout: Option[FiniteDuration]): G[Option[Chunk[Byte]]] =
          readN0(numBytes, timeout)
        def read(maxBytes: Int, timeout: Option[FiniteDuration]): G[Option[Chunk[Byte]]] =
          read0(maxBytes, timeout)
        def reads(maxBytes: Int, timeout: Option[FiniteDuration]): Stream[G, Byte] =
          Stream.eval(read(maxBytes, timeout)).flatMap {
            case Some(bytes) =>
              Stream.chunk(bytes) ++ reads(maxBytes, timeout)
            case None => Stream.empty
          }

        def write(bytes: Chunk[Byte], timeout: Option[FiniteDuration]): G[Unit] =
          write0(bytes, timeout)
        def writes(timeout: Option[FiniteDuration]): Pipe[G, Byte, Unit] =
          _.chunks.flatMap(bs => Stream.eval(write(bs, timeout)))

        def localAddress: G[SocketAddress] =
          blocker.delay[G, SocketAddress](ch.getLocalAddress)
        def remoteAddress: G[SocketAddress] =
          blocker.delay[G, SocketAddress](ch.getRemoteAddress)
        def isOpen: G[Boolean] = blocker.delay[G, Boolean](ch.isOpen)
        def close: G[Unit] = blocker.delay[G, Unit](ch.close())
        def endOfOutput: G[Unit] =
          blocker.delay[G, Unit] {
            ch.shutdownOutput(); ()
          }
        def endOfInput: G[Unit] =
          blocker.delay[G, Unit] {
            ch.shutdownInput(); ()
          }
      }
    }
    Resource.make[F, Socket[G]](socket)(_ =>
      blocker.delay[F, Unit](if (ch.isOpen) ch.close else ()).attempt.void
    )
  }
}

object SocketGroup {

  /**
    * Creates a `SocketGroup`.
    *
    * The supplied `blocker` is used for networking calls other than
    * reads/writes. All reads and writes are performed on a non-blocking thread pool
    * associated with the `SocketGroup`. The non-blocking thread pool is sized to
    * the number of available processors but that can be overridden by supplying
    * a value for `nonBlockingThreadCount`. See
    * https://openjdk.java.net/projects/nio/resources/AsynchronousIo.html for more
    * information on NIO thread pooling.
    */
  def apply[F[_]: Sync: ContextShift](
      blocker: Blocker,
      nonBlockingThreadCount: Int = 0,
      nonBlockingThreadFactory: ThreadFactory =
        ThreadFactories.named("fs2-socket-group-non-blocking", true)
  ): Resource[F, SocketGroup] =
    Resource(blocker.delay {
      val threadCount =
        if (nonBlockingThreadCount <= 0) Runtime.getRuntime.availableProcessors
        else nonBlockingThreadCount
      val acg = AsynchronousChannelGroup.withFixedThreadPool(threadCount, nonBlockingThreadFactory)
      val group = new SocketGroup(acg, blocker)
      (group, blocker.delay(acg.shutdown()))
    })
}
