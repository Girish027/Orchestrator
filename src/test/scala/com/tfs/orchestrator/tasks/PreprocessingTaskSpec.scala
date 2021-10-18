package com.tfs.orchestrator.tasks

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Date

import com.tfs.orchestrator.utils.RESOURCE_DEGREE
import org.scalatest.FlatSpec

class PreprocessingTaskSpec extends FlatSpec {
  behavior of "PreprocessingTask"

  it should "get the processing properties" in {
    val preprocessingTask = new MockPreprocessingTask
    val props = preprocessingTask.getProcessingProperties(RESOURCE_DEGREE.MEDIUM, new Date())
    val startTime = props.getProperty("START_TIME");
    val endTime = props.getProperty("END_TIME");
    val startEpochTime = props.getProperty("START_EPOCH_TIME");
    val endEpochTime = props.getProperty("END_EPOCH_TIME");
    val processorPluginJarLocation = props.getProperty("PROCESSOR_PLUGIN_JAR_LOC")
    assert(startEpochTime.equals("1517886000000"))
    assert(endEpochTime.equals("1517889600000"))
    assert(startTime.equals("201802060300"))
    assert(endTime.equals("201802060400"))
    assert(processorPluginJarLocation.equals("/user/oozie/insights/Layer2.jar,/user/oozie/insights/Layer3.jar"))
    assert(props.getProperty("num-executors").equals("15"))
    assert(props.getProperty("driver-memory").equals("3g"))
    assert(props.getProperty("executor-cores").equals("10"))
    assert(props.getProperty("executor-memory").equals("2g"))
  }

  it should "pass for optional execution properties when not provided" in {
    val preprocessingTask = new MockPreprocessingTask1
    val props = preprocessingTask.getProcessingProperties(RESOURCE_DEGREE.MEDIUM, new Date())
    val startTime = props.getProperty("START_TIME");
    val endTime = props.getProperty("END_TIME");
    val startEpochTime = props.getProperty("START_EPOCH_TIME");
    val endEpochTime = props.getProperty("END_EPOCH_TIME");
    val processorPluginJarLocation = props.getProperty("PROCESSOR_PLUGIN_JAR_LOC")
    val isPostProcessingEnabled = props.getProperty("IS_POST_PROCESSING_ENABLED")

    assert(startEpochTime.equals("1517886000000"))
    assert(endEpochTime.equals("1517889600000"))
    assert(startTime.equals("201802060300"))
    assert(endTime.equals("201802060400"))
    assert(processorPluginJarLocation.equals("/user/oozie/insights/Layer2.jar,/user/oozie/insights/Layer3.jar"))
    assert(props.getProperty("executor-cores").eq("default"))
    assert(isPostProcessingEnabled.equals("true"))
  }

  it should "evaluation of job Execution Expression " in  {

    val expectedOutput = "true"
    val preprocessingTask = new MockPreprocessingTask
    val props = preprocessingTask.getProcessingProperties(RESOURCE_DEGREE.MEDIUM, new Date())
    assert(props.getProperty("IS_JOB_VALID").equals(expectedOutput))
  }

  class MockPreprocessingTask extends PreprocessingTask {
    override def fetchCatalogSourceRangeInfo(): String = {
      val reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/samples/" +
        "catalogsourcetimerange.txt")))
      val sbuffer = new StringBuffer()
      while (reader.ready()) {
        sbuffer.append(reader.readLine())
      }
      sbuffer.toString
    }
    override def checkInputAvailability(paths: List[String]) = true
  }

    class MockPreprocessingTask1 extends PreprocessingTask {
      override def fetchCatalogSourceRangeInfo(): String = {
        val reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/samples/" +
          "catalogsourcetimerange1.txt")))
        val sbuffer = new StringBuffer()
        while (reader.ready()) {
          sbuffer.append(reader.readLine())
        }
        sbuffer.toString
      }
    override def checkInputAvailability(paths: List[String]) = true
  }

}
