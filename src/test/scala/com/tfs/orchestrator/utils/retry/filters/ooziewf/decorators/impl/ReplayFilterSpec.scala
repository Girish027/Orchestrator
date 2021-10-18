package com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.impl

import com.tfs.orchestrator.cache.Entry
import com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.{OozieFilter, OozieResponse}
import org.scalatest.FlatSpec

class ReplayFilterSpec extends FlatSpec {
  behavior of "ReplayFilterSpec"

  it should "return an non empty response" in {
    val baseFilter = new BaseFilter {
      override def filter(wfId: String): Option[OozieResponse] = {
        Option(OozieResponse("<configuration>\\r\\n  <property>\\r\\n    <name>ucInstanceDate</name>\\r\\n    <value>2019-01-07T01:00+0000</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>clientName</name>\\r\\n    <value>dish</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>user.name</name>\\r\\n    <value>oozie</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.coord.application.path</name>\\r\\n    <value>hdfs://nameserviceQAHDP/user/oozie/dp2/coord/coord.xml</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobStartTime</name>\\r\\n    <value>2019-01-06T19:00Z</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>mapreduce.job.user.name</name>\\r\\n    <value>oozie</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>viewName</name>\\r\\n    <value>View_Dimensional</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>hdfsConfigDir</name>\\r\\n    <value>/etc/hadoop/conf</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobTracker</name>\\r\\n    <value>datanode11.h2.dev.bigdata.sv2.247-inc.net:8050</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>ingestionType</name>\\r\\n    <value>ALL</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>nameNode</name>\\r\\n    <value>hdfs://nameserviceQAHDP/</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.libpath</name>\\r\\n    <value>/user/oozie/dp2/lib</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>replayTask</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>defaultPublish</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.use.system.libpath</name>\\r\\n    <value>true</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.coord.action.nominal_time</name>\\r\\n    <value>1546822800000</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>waitForInputAvailability</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>userName</name>\\r\\n    <value>SUV</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.wf.rerun.failnodes</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>cronExpression</name>\\r\\n    <value>0 * * * *</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>queueName</name>\\r\\n    <value>LongRun</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.wf.application.path</name>\\r\\n    <value>hdfs://nameserviceQAHDP/user/oozie/dp2/wf/workflow.xml</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>skipOutputCheck</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>sleepTime</name>\\r\\n    <value>120</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>hdfsPropertiesDir</name>\\r\\n    <value>/user/oozie/dp2/properties/</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>workflowForCoord</name>\\r\\n    <value>hdfs://nameserviceQAHDP/user/oozie/dp2/wf/workflow.xml</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobComplexity</name>\\r\\n    <value>medium</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobEndTime</name>\\r\\n    <value>2030-10-28T00:00Z</value>\\r\\n  </property>\\r\\n</configuration>", 5, "123", "KILLED", "Wed, 09 Jan 2019 04:39:03 GMT"))
      }
    }

    val filter = new ReplayFilter(baseFilter)
    val response = filter.filter("123")
    assert(response.isDefined)
    assert(response.get.id.equals("123"))
  }

  it should "return an empty response" in {
    val baseFilter = new OozieFilter {
      override def filter(wfId: String): Option[OozieResponse] = {
        None
      }

      override def retrieveEntryKey(responseObj: OozieResponse): Option[Entry] = {
        Option(Entry("View_Dimensional", "dish"))
      }
    }

    val filter = new ReplayFilter(baseFilter)
    val response = filter.filter("123")
    assert(response.isEmpty)
  }

  it should "return an empty response when replay task is true" in {
    val baseFilter = new BaseFilter {
      override def filter(wfId: String): Option[OozieResponse] = {
        Option(OozieResponse("<configuration>\\r\\n  <property>\\r\\n    <name>ucInstanceDate</name>\\r\\n    <value>2019-01-07T01:00+0000</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>clientName</name>\\r\\n    <value>dish</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>user.name</name>\\r\\n    <value>oozie</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.coord.application.path</name>\\r\\n    <value>hdfs://nameserviceQAHDP/user/oozie/dp2/coord/coord.xml</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobStartTime</name>\\r\\n    <value>2019-01-06T19:00Z</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>mapreduce.job.user.name</name>\\r\\n    <value>oozie</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>viewName</name>\\r\\n    <value>View_Dimensional</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>hdfsConfigDir</name>\\r\\n    <value>/etc/hadoop/conf</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobTracker</name>\\r\\n    <value>datanode11.h2.dev.bigdata.sv2.247-inc.net:8050</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>ingestionType</name>\\r\\n    <value>ALL</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>nameNode</name>\\r\\n    <value>hdfs://nameserviceQAHDP/</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.libpath</name>\\r\\n    <value>/user/oozie/dp2/lib</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>replayTask</name>\\r\\n    <value>true</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>defaultPublish</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.use.system.libpath</name>\\r\\n    <value>true</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.coord.action.nominal_time</name>\\r\\n    <value>1546822800000</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>waitForInputAvailability</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>userName</name>\\r\\n    <value>SUV</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.wf.rerun.failnodes</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>cronExpression</name>\\r\\n    <value>0 * * * *</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>queueName</name>\\r\\n    <value>LongRun</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.wf.application.path</name>\\r\\n    <value>hdfs://nameserviceQAHDP/user/oozie/dp2/wf/workflow.xml</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>skipOutputCheck</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>sleepTime</name>\\r\\n    <value>120</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>hdfsPropertiesDir</name>\\r\\n    <value>/user/oozie/dp2/properties/</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>workflowForCoord</name>\\r\\n    <value>hdfs://nameserviceQAHDP/user/oozie/dp2/wf/workflow.xml</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobComplexity</name>\\r\\n    <value>medium</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobEndTime</name>\\r\\n    <value>2030-10-28T00:00Z</value>\\r\\n  </property>\\r\\n</configuration>", 5, "123", "KILLED", "Wed, 09 Jan 2019 04:39:03 GMT"))
      }
    }

    val filter = new ReplayFilter(baseFilter)
    val response = filter.filter("123")
    assert(response.isEmpty)
  }

  it should "return an empty response when replay task doesn't exists" in {
    val baseFilter = new BaseFilter {
      override def filter(wfId: String): Option[OozieResponse] = {
        Option(OozieResponse("<configuration>\\r\\n  <property>\\r\\n    <name>ucInstanceDate</name>\\r\\n    <value>2019-01-07T01:00+0000</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>clientName</name>\\r\\n    <value>dish</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>user.name</name>\\r\\n    <value>oozie</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.coord.application.path</name>\\r\\n    <value>hdfs://nameserviceQAHDP/user/oozie/dp2/coord/coord.xml</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobStartTime</name>\\r\\n    <value>2019-01-06T19:00Z</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>mapreduce.job.user.name</name>\\r\\n    <value>oozie</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>viewName</name>\\r\\n    <value>View_Dimensional</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>hdfsConfigDir</name>\\r\\n    <value>/etc/hadoop/conf</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobTracker</name>\\r\\n    <value>datanode11.h2.dev.bigdata.sv2.247-inc.net:8050</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>ingestionType</name>\\r\\n    <value>ALL</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>nameNode</name>\\r\\n    <value>hdfs://nameserviceQAHDP/</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.libpath</name>\\r\\n    <value>/user/oozie/dp2/lib</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>defaultPublish</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.use.system.libpath</name>\\r\\n    <value>true</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.coord.action.nominal_time</name>\\r\\n    <value>1546822800000</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>waitForInputAvailability</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>userName</name>\\r\\n    <value>SUV</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.wf.rerun.failnodes</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>cronExpression</name>\\r\\n    <value>0 * * * *</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>queueName</name>\\r\\n    <value>LongRun</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>oozie.wf.application.path</name>\\r\\n    <value>hdfs://nameserviceQAHDP/user/oozie/dp2/wf/workflow.xml</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>skipOutputCheck</name>\\r\\n    <value>false</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>sleepTime</name>\\r\\n    <value>120</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>hdfsPropertiesDir</name>\\r\\n    <value>/user/oozie/dp2/properties/</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>workflowForCoord</name>\\r\\n    <value>hdfs://nameserviceQAHDP/user/oozie/dp2/wf/workflow.xml</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobComplexity</name>\\r\\n    <value>medium</value>\\r\\n  </property>\\r\\n  <property>\\r\\n    <name>jobEndTime</name>\\r\\n    <value>2030-10-28T00:00Z</value>\\r\\n  </property>\\r\\n</configuration>", 5, "123", "KILLED", "Wed, 09 Jan 2019 04:39:03 GMT"))
      }
    }

    val filter = new ReplayFilter(baseFilter)
    val response = filter.filter("123")
    assert(response.isEmpty)
  }
}
