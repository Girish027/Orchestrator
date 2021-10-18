package com.tfs.orchestrator.utils

import java.io.{BufferedReader, InputStream, InputStreamReader, Reader}

object Utils {

  def readInputReader(reader: Reader): String = {
    val specBuilder = new StringBuilder
    var specReader: BufferedReader = null
    try {
      specReader = new BufferedReader(reader)
      while (specReader.ready()) {
        specBuilder.append(specReader.readLine)
      }
    }
    finally {
      specReader.close()
    }
    specBuilder.toString
  }

  def splitAndConvertToList(value:String):List[String]=
  {
    value.split(",").toList
  }

}
