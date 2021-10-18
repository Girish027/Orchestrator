package com.tfs.orchestrator.utils

import java.io.File
import java.util.Properties

import org.scalatest.FlatSpec

class OozieUtilsSpec extends FlatSpec {
  behavior of "OozieUtils"

  it should "save the oozie context, given a key and value" in {
    //Set the property to a dummy file
    System.setProperty(Constants.PROP_OOZIE_ACTION_OUTPUT_CONTEXT, "hello.txt")

    //Save the context to Oozie
    OozieUtils.saveToOozieContext("hello", "world")

    //verify the properties by reading back from Oozie context
    val file = new File("hello.txt")
    val props = PropertyReader.getProperties(file)
    assert(props.getProperty("hello").equals("world"))
    file.delete()
  }

  it should "save the oozie context, given a Properties object" in {
    //Set the property to a dummy file
    val testProperties = new Properties()
    testProperties.setProperty("hello", "world")
    testProperties.setProperty("hello1", "world1")
    System.setProperty(Constants.PROP_OOZIE_ACTION_OUTPUT_CONTEXT, "hello.txt")

    //Save the context to Oozie
    OozieUtils.saveToOozieContext(testProperties)

    //verify the properties by reading back from Oozie context
    val file = new File("hello.txt")
    val props = PropertyReader.getProperties(file)
    assert(props.getProperty("hello").equals("world"))
    assert(props.getProperty("hello1").equals("world1"))
    file.delete()
  }
}
