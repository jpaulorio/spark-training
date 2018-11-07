package com.thoughtworks.utils

import java.util.UUID

import com.thoughtworks.models.{Order, Product, Store}

object DataGenerator {

  private val ORDER_COUNT = 2

  def main(args: Array[String]): Unit = {
    val availableStores = List(
      Store(UUID.randomUUID().toString, "Loja Sul"),
      Store(UUID.randomUUID().toString, "Loja Norte"),
      Store(UUID.randomUUID().toString, "Loja Leste"),
      Store(UUID.randomUUID().toString, "Loja Oeste")
    )
    val availableProducts = List(
      Product(UUID.randomUUID().toString, "Bicicleta", "Esporte & Lazer", 234.56),
      Product(UUID.randomUUID().toString, "Bola de Futebol", "Esporte & Lazer", 34.56),
      Product(UUID.randomUUID().toString, "Televisão", "Eletrônicos", 234.56),
      Product(UUID.randomUUID().toString, "Celular", "Eletrônicos", 1234.56),
      Product(UUID.randomUUID().toString, "Geladeira", "Eletrodomésticos", 2465.67),
      Product(UUID.randomUUID().toString, "Fogão", "Eletrodomésticos", 356.78),
      Product(UUID.randomUUID().toString, "Videogame", "Eletrônicos", 3567.89),
      Product(UUID.randomUUID().toString, "Camisa", "Vestuário", 56.79),
      Product(UUID.randomUUID().toString, "Calça", "Vestuário", 89.01),
      Product(UUID.randomUUID().toString, "Vinho", "Bebidas", 67.89)
    )

    val productLines = availableProducts.map(product => product.productToCSVString())
    val bwProducts = FileUtils.initializeFile("products.csv", "ProductId;Name;Category;Price")
    FileUtils.writeToFile(productLines, bwProducts)
    FileUtils.closeFile(bwProducts)

    val bwOrders = FileUtils.initializeFile("orders.csv", "OrderId;CustomerId;Timestamp;StoreId")
    val bwOrderItems = FileUtils.initializeFile("orderItems.csv", "OrderId;ProductId;Discount;Quantity")
    for (_ <- 1 to ORDER_COUNT) {
      val order = Order.generateRandom(availableStores, availableProducts)
      val orderLine = order.orderToCSVString()
      val itemsLines = order.itemsToCSVString()
      FileUtils.writeToFile(orderLine, bwOrders)
      FileUtils.writeToFile(itemsLines, bwOrderItems)
    }

    FileUtils.closeFile(bwOrders)
    FileUtils.closeFile(bwOrderItems)

    Console.println("Finished generating files.")
  }


}
