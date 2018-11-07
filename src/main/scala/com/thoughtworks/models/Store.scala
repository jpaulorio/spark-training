package com.thoughtworks.models

case class Store(id:String, name:String) {
  def storeToCSVString(): String = {
    s"""$id;$name;"""
  }
}
