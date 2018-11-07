package com.thoughtworks.models

case class Product(id:String, name:String, category:String, price:Double) {
  def productToCSVString(): String = {
    s"""${this.id};${this.name};${this.category};${this.price}\n"""
  }
}
