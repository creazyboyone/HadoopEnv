package com.feng.ml.utils

import java.io.File

class FileUtils {
  def getDataFile(file: String): String = {
    "/user/admin/data/" + file
  }

  def getModelFile(file: String): String = {
    "/user/admin/model/" + file
  }

  def deleteFile(path: String): Unit = {
    val file = new File(path)
    deleteAll(file)
  }

  private def deleteAll(file: File): Unit = {
    if (file.isDirectory) {
      val files = file.listFiles()
      for (f <- files) {
        deleteAll(f)
      }
      file.delete()
    } else if (file.isFile) {
      file.delete()
    }
  }

}

