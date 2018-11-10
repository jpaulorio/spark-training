package com.thoughtworks.utils

import java.io._
import java.net.ServerSocket
import java.util.UUID

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object SocketServer {
  def main(args: Array[String]): Unit = {
    runGeneratorOnSocket(9999, () => UUID.randomUUID().toString)

    while (true) {}
  }
  def runGeneratorOnSocket(port:Integer, generator: () => Any):Future[Any] = {
    Future {
      val server = new ServerSocket(port)

      println(s"Server initialized at port: $port")

      val client = server.accept
      val printStream: PrintStream = new PrintStream(client.getOutputStream)

      while (true) {
        val message = generator()
        printStream.println(message)
//        println(s"Server sent: " + message)
        Thread.sleep(50)
      }
    }
  }
}