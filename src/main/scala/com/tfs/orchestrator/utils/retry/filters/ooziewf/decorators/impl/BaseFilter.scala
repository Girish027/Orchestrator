package com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.impl

import com.tfs.orchestrator.cache.Entry
import com.tfs.orchestrator.utils.{Constants, OozieUtils}
import com.tfs.orchestrator.utils.retry.filters.ooziewf.decorators.{OozieFilter, OozieResponse}
import net.liftweb.json._

class BaseFilter extends OozieFilter {

  private val rerunStates = List("SUCCEEDED", "KILLED", "FAILED")
  private var entry: Option[Entry] = Option.empty

  override def filter(wfId: String): Option[OozieResponse] = {
    val oozieResponse = fetchWFStatus(wfId)
    if (!oozieResponse.isEmpty) {
      val json = parse(oozieResponse.get)
      implicit val formats: DefaultFormats.type = DefaultFormats
      val responseObj = json.extract[OozieResponse]
      if (rerunStates.contains(responseObj.status)) {
        generateEntryKey(responseObj)
        return Option(responseObj)
      }
    }
    return None
  }

  override def retrieveEntryKey(responseObj: OozieResponse): Option[Entry] = {
    if(entry.isEmpty){
      generateEntryKey(responseObj)
    }
    entry
  }

  protected def fetchWFStatus(wfId: String): Option[String] = {
    OozieUtils.fetchOozieWFStatus(wfId)
  }

  private def generateEntryKey(responseObj: OozieResponse): Unit = {
    val viewName = OozieUtils.parse(responseObj.conf, Constants.KEY_VIEW_NAME)
    val clientName = OozieUtils.parse(responseObj.conf, Constants.KEY_CLIENT_NAME)
    if (viewName.isDefined && clientName.isDefined) {
      entry = Option(Entry(viewName.get, clientName.get))
    }
  }
}
