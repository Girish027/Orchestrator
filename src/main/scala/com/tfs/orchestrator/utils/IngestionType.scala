package com.tfs.orchestrator.utils

/**
 * Created by bikesh.singh on 14-09-2018.
 */
object IngestionType extends Enumeration{
  type IngestionType = Value

  val ES = Value("ES")
  val DRUID = Value("DRUID")
  val SFTP = Value("SFTP")
  val ALL = Value("ALL")

  override def toString(): String ={
    var ingestionTypes: String =""
    IngestionType.values.foreach(value =>{ingestionTypes+= value+","})
    ingestionTypes = ingestionTypes.substring(0, ingestionTypes.length-1)
    ingestionTypes
  }
}
