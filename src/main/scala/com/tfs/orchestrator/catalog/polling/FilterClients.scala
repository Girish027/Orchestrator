package com.tfs.orchestrator.catalog.polling

import com.tfs.orchestrator.utils.{Constants, PropertyReader}

trait FilterClients {

  var successor=None:Option[FilterClients]
   /*
  it will add client to list based on filters on ClientExecutionProperties and  return list.
   */
  def applyFilter(clientExecProps: List[ClientExecutionProperties], list: List[String]): List[String]

  def getEnvironmentValue(environmentProperties: Option[Map[String,String]], propertyName:String, defaultValue :String):String ={
    var propertyVal = defaultValue
    if (environmentProperties.isDefined)
      propertyVal = environmentProperties.get.getOrElse("dataCenter",defaultValue)
    return propertyVal
  }
}

class FilterClientsByDataCenter() extends FilterClients {

  private val dataCenter = PropertyReader.getSystemProperties().getProperty(Constants.DATA_CENTER_NAME)

  override def applyFilter(clientExecProps:  List[ClientExecutionProperties], list: List[String]): List[String] = {
    var allClients = list
    clientExecProps foreach (clientProps => {
      val dc = getEnvironmentValue(clientProps.environmentProperties, "dataCenter", dataCenter)
      if (dc.toLowerCase.split(Constants.DEFAULT_LIST_SEPARATOR).contains(dataCenter.toLowerCase))
        allClients = clientProps.name :: allClients
    })
    return successor.getOrElse(new NoFilterClients).applyFilter(clientExecProps, allClients)
  }

}
class NoFilterClients() extends FilterClients{
  override def applyFilter(clientExecProps: List[ClientExecutionProperties], list: List[String]): List[String] = {
    return list
  }
}
