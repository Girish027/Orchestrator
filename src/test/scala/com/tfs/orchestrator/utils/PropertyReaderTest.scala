package com.tfs.orchestrator.utils

import com.tfs.orchestrator.exceptions.InitializationException
import org.scalatest.{SequentialNestedSuiteExecution, FlatSpec}

class PropertyReaderTest extends FlatSpec with SequentialNestedSuiteExecution {

  behavior of "PropertyReader"

  it should "initialize properly given the necessary property files" in {
    PropertyReader.initialize(false, "src/main/resources/properties")
  }

  it should "provide System Properties as set in the property file" in {
    val sysProps = PropertyReader.getSystemProperties()
    assert(sysProps != null)
    assert(sysProps.getProperty("job.tracker") == "psr-dap-druid02.app.shared.int.sv2.247-inc.net:8050")
    assert(sysProps.getProperty("name.node") == "hdfs://psr-dap-druid02.app.shared.int.sv2.247-inc.net:8020")
  }

  it should "provide Oozie Properties as set in the property file" in {
    val oozieProps = PropertyReader.getOozieProperties()
    assert(oozieProps != null)
    assert(oozieProps.getProperty("oozie.use.system.libpath").equals("true"))
    assert(oozieProps.getProperty("oozie.client.url").equals("http://psr-dap-druid02.app.shared.int.sv2.247-inc.net:11000/oozie"))
    assert(oozieProps.getProperty("oozie.default.workflow.path").equals("hdfs://nameserviceQAHDP/user/oozie/dp2/wf"))
    assert(oozieProps.getProperty("oozie.default.coord.path").equals("hdfs://nameserviceQAHDP/user/oozie/dp2/coord"))
    assert(oozieProps.getProperty("oozie.default.scripts.path").equals("hdfs://nameserviceQAHDP/user/oozie/dp2/scripts"))
  }

  it should "provide Application Properties as set in the property file" in {
    val appProps = PropertyReader.getApplicationProperties()
    assert(appProps != null)
    assert(appProps.getProperty("catalog.snapshot.url").equals("http://host298.assist.pool.sv2.247-inc.net:8080/catalog/orchestrator/snapshot"))
    assert(appProps.getProperty("catalog.polling.interval").equals("10000"))
  }

  it should "fail on providing incorrect properties location with InitializationException" in {
    intercept[InitializationException] {
      PropertyReader.initialize(false, "src/random")
    }
  }

  it should "return value for the given key" in {
    PropertyReader.initialize(false, "src/test/resources/properties")
    //assert(PropertyReader.getApplicationPropertiesValue[String]("sla.es.dataType","jobStats").equals("jobStats2"))
    assert(PropertyReader.getApplicationPropertiesValue[String]("sla.es.dataType","dp2-jobmetrics*").equals("dp2-jobmetrics*"))
    assert(PropertyReader.getApplicationPropertiesValue[Long]("http.read.timeout",20000L).equals(10000L))
    assert(PropertyReader.getApplicationPropertiesValue[Double]("double.number",25).equals("25.25".toDouble))
    assert(PropertyReader.getApplicationPropertiesValue[Float]("double.number",25).equals("25.25".toFloat))
    assert(PropertyReader.getApplicationPropertiesValue[Int]("http.read.timeout",20000).equals("10000".toInt))
    assert(PropertyReader.getApplicationPropertiesValue[Byte]("byte.number",125).equals("25".toByte))
  }


  it should "return default value for the given non existent key" in {
    PropertyReader.initialize(false, "src/test/resources/properties")
    assert(PropertyReader.getApplicationPropertiesValue[Boolean]("boolean.value",false).equals(false))
  }

  it should "return default value for an invalid type" in {
    PropertyReader.initialize(false, "src/test/resources/properties")
    val ex = new RuntimeException
    assert(PropertyReader.getApplicationPropertiesValue[RuntimeException]("boolean.value",ex).equals(ex))
  }

}
