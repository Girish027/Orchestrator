package com.tfs.orchestrator.hibernate.entities

import java.util.Date
import javax.persistence._

import com.tfs.orchestrator.utils.{Constants, DateUtils}

/**
  * DTO to define the views table in the database
  * This table is used for storing all the view configurations in the mysql, which can be later used to compare if there
  * are any changes done when we receive the new snapshot while polling
  */

@Entity
@Table(name = "views")
class View {

  @Id
  var id: String = _

  @Column(name = "view_name")
  var viewName: String = _

  // if multiple clients are configured for the same view use comma separated clientids
  @Column(name = "client_ids")
  var clientIds : String = _

  @Column(name = "cron_expression")
  var cronExpression : String = _

  @Column(name = "user_name")
  var userName: String = _

  @Column(name = "job_start_time")
  var jobStartTime: String = _

  @Column(name = "job_end_time")
  var jobEndTime: String = _

  @Column(name = "complexity")
  var jobComplexity: String = _

  @Column(name = "date_created")
  var dateCreated: Date = _

  @Column(name = "date_modified")
  var dateModified: Date = _

  @Column(name = "data_center")
  var dataCenter: String = _


  /**
    * Used to compare two objects of View type
    * @param s - object to be compared with
    * @return - Returns true if the objects are matched and false if not
    */
  def compareWith(s: View) : Boolean = {
      return this.userName.equals(s.userName) &&
              this.jobStartTime.equals(s.jobStartTime) &&
              this.jobEndTime.equals(s.jobEndTime) &&
              this.jobComplexity.equals(s.jobComplexity)
  }


  /**
    * Used to compare two objects of View type
    * @param that - object to be compared with
    * @return - Returns true if the objects are matched and false if not
    */
  def compareJobEndWith(that: View) : Boolean = {
    return this.jobEndTime.equals(that.jobEndTime)
  }


  def compareClients(s: View) :Boolean = {
    return   this.clientIds.equals(s.clientIds)
  }
  /**
    * Method to update one view using the values of the other view
    * @param view
    */
  def updateView(view: View): Unit ={
    this.userName = view.userName
    this.cronExpression = view.cronExpression
    this.jobStartTime = view.jobStartTime
    this.jobEndTime = view.jobEndTime
    this.jobComplexity = view.jobComplexity
    this.clientIds = view.clientIds
  }

  override def toString : String = s"View Details: viewname: ${viewName}, cron: ${cronExpression}, " +
    s"jobComplexity: ${jobComplexity} start:" +
    s" ${DateUtils.epochToUTCString(Constants.OOZIE_DATE_FORMAT, jobStartTime.toLong)}, " +
    s"end: ${DateUtils.epochToUTCString(Constants.OOZIE_DATE_FORMAT, jobEndTime.toLong)}, clientIds:[${clientIds}], " +
    s"dataCenter: $dataCenter"
}

object View {
}
