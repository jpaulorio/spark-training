package com.thoughtworks.utils

import java.io._
import java.net.ServerSocket
import java.util.UUID

import com.thoughtworks.models.{Order, Product, Store}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object SocketServer {
  def main(args: Array[String]): Unit = {
    if (args.size == 0) {
      println("É necessário passar a porta como argumento.")
      return
    }

    val port = Integer.parseInt(args(0))
    val availableProducts = Product.getAvailableProducts()

    runGeneratorOnSocket(port,
      () => {
        val order = Order.generateRandom(Store.getAvailableStores(), availableProducts)
        val orderString = order.orderToCSVString().replace("\n", "")
        val orderItemsString = order.itemsToCSVString().mkString("|")
          .replace(';', ',').replace("\n", "")
        orderString + ";" + orderItemsString
      }
    )

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