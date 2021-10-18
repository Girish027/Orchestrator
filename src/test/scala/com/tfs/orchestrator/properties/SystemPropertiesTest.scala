package com.tfs.orchestrator.properties

import com.tfs.orchestrator.utils.{PropertyReader, RESOURCE_DEGREE}
import org.scalatest.FlatSpec

class SystemPropertiesTest extends FlatSpec {
  behavior of "SystemProperties"
  PropertyReader.initialize(false,"src/test/resources/properties")

  object MockSystemProperties extends SystemProperties {
  }

  it should "pick the appropriate properties from system.properties file" in {
    assert(MockSystemProperties.jobTracker.equals("psr-dap-druid02.app.shared.int.sv2.247-inc.net:8050"))
    assert(MockSystemProperties.nameNode.equals("hdfs://psr-dap-druid02.app.shared.int.sv2.247-inc.net:8020"))
    assert(MockSystemProperties.idmPath.equals("${nameNode}/raw/prod/rtdp/idm/events/${clientName}/year=${YEAR}/month=${MONTH}/day=${DAY}/hour=${HOUR}/min=${MINUTE}"))
    assert(MockSystemProperties.speechPath.equals("${nameNode}/raw/prod/rtdp/speech/${clientName}/year=${YEAR}/month=${MONTH}/day=${DAY}/hour=${HOUR}/min=${MINUTE}"))
  }

  it should "provide appropriate size profiling" in {
    val sysProps = MockSystemProperties
    assert(sysProps.getDataSizeDegree(6000).equals(RESOURCE_DEGREE.LOW))
    assert(sysProps.getDataSizeDegree(10000).equals(RESOURCE_DEGREE.MEDIUM))
    assert(sysProps.getDataSizeDegree(20000).equals(RESOURCE_DEGREE.HIGH))
  }

  it should "provide appropriate resource profiling" in {
    val sysProps = MockSystemProperties
    assert(sysProps.getResourceProfile(RESOURCE_DEGREE.LOW, RESOURCE_DEGREE.LOW).equals(Some((5, 3))))
    assert(sysProps.getResourceProfile(RESOURCE_DEGREE.LOW, RESOURCE_DEGREE.MEDIUM).equals(Some((5, 15))))
    assert(sysProps.getResourceProfile(RESOURCE_DEGREE.LOW, RESOURCE_DEGREE.HIGH).equals(Some((5, 32))))
    assert(sysProps.getResourceProfile(RESOURCE_DEGREE.MEDIUM, RESOURCE_DEGREE.LOW).equals(Some((10, 3))))
    assert(sysProps.getResourceProfile(RESOURCE_DEGREE.MEDIUM, RESOURCE_DEGREE.MEDIUM).equals(Some((10, 15))))
    assert(sysProps.getResourceProfile(RESOURCE_DEGREE.MEDIUM, RESOURCE_DEGREE.HIGH).equals(Some((10, 32))))
    assert(sysProps.getResourceProfile(RESOURCE_DEGREE.HIGH, RESOURCE_DEGREE.LOW).equals(Some((15, 3))))
    assert(sysProps.getResourceProfile(RESOURCE_DEGREE.HIGH, RESOURCE_DEGREE.MEDIUM).equals(Some((15, 15))))
    assert(sysProps.getResourceProfile(RESOURCE_DEGREE.HIGH, RESOURCE_DEGREE.HIGH).equals(Some((20, 32))))
  }
}
