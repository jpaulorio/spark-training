package com.thoughtworks.utils

import java.io.{BufferedWriter, File, FileWriter}
;

case class FileUtils() {
}

object FileUtils {
  def initializeFile(fileName: String, header: String): BufferedWriter = {
    val file = new File(fileName)
    val bufferedWriter = new BufferedWriter(new FileWriter(file))
    bufferedWriter.write(header + "\n")
    bufferedWriter
  }

  def writeToFile(line: String, bufferedWriter: BufferedWriter) = {
    bufferedWriter.write(line)
  }

  def writeToFile(lines: List[String], bufferedWriter: BufferedWriter) = {
    lines.foreach(bufferedWriter.write)
  }

  def closeFile(bufferedWriter: BufferedWriter): Unit = {
    bufferedWriter.close()
  }
}
