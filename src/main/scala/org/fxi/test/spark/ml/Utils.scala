package org.fxi.test.spark.ml

import java.io.{File, IOException}
import java.util.UUID

/**
  * Created by seki on 16/10/26.
  */
object Utils {
  def createTempDir(): File = {
    var dir: File = null
    try {
      dir = new File("./tempFile", "tmp" + "-" + UUID.randomUUID.toString)
      if (dir.exists() || !dir.mkdirs()) {
        dir = null
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    dir.getCanonicalFile
  }

  def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory ) {
          var savedIOException: IOException = null
          for (child <- file.listFiles()) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

}
