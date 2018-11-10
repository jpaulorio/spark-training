package com.thoughtworks.utils

import java.util.UUID

import com.thoughtworks.models.{Order, Product, Store}

object DataGenerator {

  private val ORDER_COUNT = 2

  def main(args: Array[String]): Unit = {
    val availableStores = Store.getAvailableStores()

    val availableProducts = Product.getAvailableProducts()

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
