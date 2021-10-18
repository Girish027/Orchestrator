package com.tfs.orchestrator.jobs.dag.oozie

import java.io.IOException

import com.tfs.orchestrator.exceptions.InitializationException
import com.tfs.orchestrator.properties.SystemProperties
import com.tfs.orchestrator.utils.{Constants, HadoopUtils, PropertyReader}
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.scala.Logging

import scala.util.control.NonFatal

/**
 * This class is responsible for creating the Oozie workflows and optionally save them on the hadoop.
 *
 * For now, we assume a template for coordinator and workflow that reside in the hadoop. In future, this
 * class may be amended to create custom workflows.
 *
 */
object OozieDagCreater extends Logging {

  private val oozieProperties = PropertyReader.getOozieProperties
  private var dagLocations: DAGLocations = DAGLocations("", "")
  private var dagIngestLocations: DAGLocations = DAGLocations("", "")

  /**
   * 1. Creates the coordinator template into HDFS, if not already available in the configured directory.
   * 2. Creates the workflow template into HDFS, if not already present in the configured directory.
   * 3. Creates the processing and sleep scripts into HDFS, if not already present in the configured directory.
   */
  def initialize(systemProperties: SystemProperties): Unit  = {
    //Local Location of the templates and scripts for Orchestrator to place them in hadoop.
    val templatesPath = System.getProperty(Constants.COMMAND_TEMPLATES_DIR_KEY)
    val scriptsPath = System.getProperty(Constants.COMMAND_SCRIPTS_DIR_KEY)

    //Hadoop Locations to which the workflow, coord and scripts need to be copied.
    val defWorkflowPath = oozieProperties.getProperty(Constants.PROP_OOZIE_DEFAULT_WORKFLOW_PATH)
    val defCoordPath = oozieProperties.getProperty(Constants.PROP_OOZIE_DEFAULT_COORD_PATH)
    val defScriptsPath = oozieProperties.getProperty(Constants.PROP_OOZIE_DEFAULT_SCRIPTS_PATH)

    if (defWorkflowPath == null || defCoordPath == null || defScriptsPath == null) {
      throw new InitializationException("Mising default workflow and coord directories.")
    }

    try {
      //Copy Workflow xml if it does not exist
      HadoopUtils.copyFilesToHDFSIfAbsent(templatesPath, defWorkflowPath, Constants.FILE_WORKFLOW_XML)

      //Copy Coordinator xml if it does not exist
      HadoopUtils.copyFilesToHDFSIfAbsent(templatesPath, defCoordPath, Constants.FILE_COORD_XML)

      //Copy process script if it does not exist
      HadoopUtils.copyFilesToHDFSIfAbsent(scriptsPath, defScriptsPath, Constants.FILE_PROCESSING_SCRIPT)
      HadoopUtils.copyFilesToHDFSIfAbsent(scriptsPath, defScriptsPath, Constants.FILE_SLEEP_SCRIPT)
      HadoopUtils.copyFilesToHDFSIfAbsent(scriptsPath, defScriptsPath, Constants.FILE_INGEST_SCRIPT)
    } catch {
      case ex:IOException => {
        logger.error("Unexpected error while setting the oozie environment.")
        throw new InitializationException(ex, "");
      }
      case NonFatal(ex) => {
        logger.error(s"Incorrect path entries. Verify workflow and coord in directories: ${defWorkflowPath} and"
          + s" ${defCoordPath}")
        throw new InitializationException(ex, "")
      }
    }

    dagLocations = DAGLocations (new Path(defWorkflowPath, Constants.FILE_WORKFLOW_XML).toString,
      new Path(defCoordPath, Constants.FILE_COORD_XML).toString)
    dagIngestLocations = DAGLocations (new Path(defWorkflowPath, Constants.FILE_INGEST_WORKFLOW_XML).toString,
      new Path(defCoordPath, Constants.FILE_COORD_XML).toString)
  }

  /**
   * The default workflow templates.
   * @return
   */
  def getDefaultDagLocations(): DAGLocations = dagLocations
  
  /**
   * The ingest workflow template.
   * 
   */
  def getIngestDagLocations(): DAGLocations = dagIngestLocations

}

/**
 * Locations of workflow and the coordinator.
 * @param workflowLocation
 * @param coordLocation
 */
case class DAGLocations(workflowLocation: String, coordLocation: String)
