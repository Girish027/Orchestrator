package com.tfs.orchestrator.hibernate.entities

import com.tfs.orchestrator.utils.{Constants, DateUtils}
import javax.persistence._

/**
 * We save the cron job id against each client and view name. This will be used to kill
 * the jobs. This can be used to further queries in future.
 */
@Entity
@Table(name = "view_client_jobIds")
class ViewClientJobId {

  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  var id: Int = _

  @Column(name = "view_name")
  var viewName: String = _

  // if multiple clients are configured for the same view use comma separated clientids
  @Column(name = "client_id")
  var clientId : String = _

  // if multiple clients are configured for the same view use comma separated clientids
  @Column(name = "job_id")
  var jobId : String = _

  // job start time in epoch millis.
  @Column(name = "job_start_epoch")
  var jobStartEpochTime : Long = _

  // job end time in epoch millis.
  @Column(name = "job_end_epoch")
  var jobEndEpochTime : Long = _

  @Column(name = "cron_expression")
  var cronExpression : String = _

  override def toString : String = s"Job Details: viewname: ${viewName}, cron: ${cronExpression}, " +
    s"clientId: ${clientId} jobStartEpochTime:" +
    s" $jobStartEpochTime, " +
    s"jobEndEpochTime: $jobEndEpochTime]"
}
