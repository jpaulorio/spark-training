package utils

import java.io._
import java.net.ServerSocket

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object SocketServer {
  def runGeneratorOnSocket(port:Integer, generator: () => Any):Future[Any] = {
    Future {
      val server = new ServerSocket(port)

      println(s"Server initialized at port: $port")

      val client = server.accept
      val printStream: PrintStream = new PrintStream(client.getOutputStream)

      while (true) {
        val message = generator()
        printStream.println(message)
        println(s"Server sent: " + message)
      }
    }
  }
}