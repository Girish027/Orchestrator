package com.tfs.orchestrator.utils

import java.io.{BufferedReader, InputStream, InputStreamReader}

import com.tfs.orchestrator.utils.SlaRecordPublish.propertiesDir
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.logging.log4j.scala.Logging

object HadoopUtils extends Logging{

  private val hadoopConf = new Configuration()
  val configLocation = System.getProperty("hadoop.config.dir")
  hadoopConf.addResource(new Path(configLocation, "hdfs-site.xml"))
  hadoopConf.addResource(new Path(configLocation, "core-site.xml"))
  val hdfs = FileSystem.get(hadoopConf)

  /**
   * Checks the existence of file in the given locations.
   * @param successFileName
   * @param directories string containing dirs separated by COMMA(,)
   * @return
   */
  def hadoopFileExists(successFileName: String, directories: String): Boolean = {
    val exists = directories.split(",").map(new Path(_, successFileName)).map(hdfs.exists(_)).foldLeft(true)(_ && _)
    exists
  }

  /**
   * Checks the existence of file in the given locations.
   * @param successFileName
   * @param directories string containing dirs separated by COMMA(,)
   * @return
   */
  def hadoopFileExists(successFileName: String, directories: List[String]): Boolean = {
    val exists = directories.map(new Path(_, successFileName)).map(hdfs.exists(_)).foldLeft(true)(_ && _)
    exists
  }

  /**
   * Checks the existence of file.
   * @param fileName
   * @return
   */
  def hadoopFileExists(fileName: String): Boolean = {
    val filePath = new Path(fileName)
    hdfs.exists(filePath)
  }

  /**
   * Calculates the size of the given directory.
   * @param directoryPath
   * @return
   */
  def sizeOfDirectory(directoryPath: String): Long = {
    val size = hdfs.getContentSummary(new Path(directoryPath)).getLength()

    size
  }

  /**
   * Get the InputStream for the given file path.
   * @param directoryName
   * @param fileName
   * @return
   */
  def getInputStream(directoryName: String, fileName: String): InputStream = {
    return hdfs.open(new Path(directoryName, fileName))
  }

  /**
   * Copy files from local file system to remote location
   * @param localPath
   * @param remotePath
   * @param fileName
   */
  def copyFilesToHDFSIfAbsent(localPath: String, remotePath: String, fileName: String): Unit = {
    if (!hdfs.exists(new Path(remotePath, fileName))) {
      hdfs.copyFromLocalFile(false, false,
        new Path(localPath, fileName),
        new Path(remotePath, fileName))
    }
  }


  /**
    * Reads files from HDFS filesystem
    * @param fileName
    * @return String representation of the file.
    */

  def readFiles(fileName: String): String = {
    Utils.readInputReader(new InputStreamReader(HadoopUtils.getInputStream(propertiesDir,
      fileName)))
  }
}
