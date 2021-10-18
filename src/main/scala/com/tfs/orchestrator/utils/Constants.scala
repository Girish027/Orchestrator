package com.tfs.orchestrator.utils

/**
 * 1. Each name starting with PROP indicates the properties with which values are saved.
 * 2. Each name starting with KEY indicates the properties which need to be passed/set.
 * 3. Each name starting with DEFAULT meant for default values
 * 4. Each name starting with FILE indicates file names
 * 5. Each name starting with COMMAND indicates command line properties.
 * 4. All others are various constants used in the code with a prefix indicating usage component.
 */
object Constants {

  //COMMAND LINE PROPERTIES
  val COMMAND_PROPS_DIR_KEY = "properties.dir";
  val COMMAND_TEMPLATES_DIR_KEY = "templates.dir";
  val COMMAND_SCRIPTS_DIR_KEY = "scripts.dir";

  //Property file names
  val FILE_SYSTEM_PROPERTIES = "system.properties"
  val FILE_OOZIE_PROPERTIES = "oozie.properties"
  val FILE_HIBERNATE_CONFIG = "hibernate.cfg.xml"
  val FILE_APPLICATION_PROPERTIES = "application.properties"
  val FILE_WORKFLOW_XML = "workflow.xml"
  val FILE_INGEST_WORKFLOW_XML = "ingest_workflow.xml"
  val FILE_COORD_XML = "coord.xml"
  val FILE_PROCESSING_SCRIPT = "process_script.sh"
  val FILE_SLEEP_SCRIPT = "sleep_script.sh"
  val FILE_INGEST_SCRIPT = "ingest_script.sh"

  //System properties
  val PROP_SYSTEM_JOB_TRACKER= "job.tracker"
  val PROP_SYSTEM_NAME_NODE = "name.node"
  val PROP_SYSTEM_DEFAULT_USER = "default.user.name"
  val PROP_SYSTEM_IDM_PATH = "idm.path"
  val PROP_SYSTEM_SPEECH_PATH = "speech.path"
  val PROP_SYSTEM_VALIDATE_HADOOP = "validate.hadoop"
  val PROP_SYSTEM_HADOOP_LOCATION = "validate.hadoop.location"
  val DATA_CENTER_NAME = "data.center"

  //Oozie properties
  val PROP_OOZIE_DEFAULT_QUEUE = "oozie.queue.name"
  val PROP_OOZIE_CLIENT_URL ="oozie.client.url"
  val PROP_OOZIE_ACTION_OUTPUT_CONTEXT = "oozie.action.output.properties"
  val PROP_OOZIE_DEFAULT_WORKFLOW_PATH = "oozie.default.workflow.path"
  val PROP_OOZIE_DEFAULT_COORD_PATH = "oozie.default.coord.path"
  val PROP_OOZIE_DEFAULT_SCRIPTS_PATH = "oozie.default.scripts.path"

  //Other properties
  val PROP_INPUT_WAIT = "default.input.wait"
  val PROP_OUTPUT_CHECK = "default.output.check"

  //Constants
  val SYSTEM_RESOURCE_DEF_PREFIX = "resource"
  val SYSTEM_RESOURCE_SIZE= "size"
  val SYSTEM_RESOURCE_DEF_DELIMITER = "."
  val SYSTEM_RESOURCE_DEGREE_LOW = "low"
  val SYSTEM_RESOURCE_DEGREE_MEDIUM = "medium"
  val SYSTEM_RESOURCE_DEGREE_HIGH = "high"

  //Default property values.
  val DEFAULT_USER_NAME = "oozie"
  val DEFAULT_JOB_END_DURATION = 86400000
  var DEFAULT_LIST_SEPARATOR = ","

  //Oozie Property Keys
  val KEY_OOZIE_COORD_PATH = "oozie.coord.application.path"
  val KEY_OOZIE_WORKFLOW_PATH = "oozie.wf.application.path"
  val KEY_OOZIE_LIBPATH ="oozie.libpath"
  val KEY_OOZIE_USE_LIBPATH = "oozie.use.system.libpath"
  val KEY_OOZIE_WF_FOR_COORD = "workflowForCoord"
  val KEY_QUEUE_NAME = "queueName"
  val KEY_CLIENT_NAME = "clientName"
  val KEY_JOB_START_TIME ="jobStartTime"
  val KEY_JOB_END_TIME = "jobEndTime"
  val KEY_JOB_CRON_EXPR = "cronExpression"
  val KEY_VIEW_NAME ="viewName"
  val KEY_USER_NAME = "userName"
  val KEY_JOB_COMPLEXITY = "jobComplexity"
  val KEY_DEFAULT_PUBLISH = "defaultPublish"
  val KEY_OOZIE_USER_NAME = "user.name"
  val KEY_REPLAY_TASK = "replayTask"
  val KEY_SLEEP_TIME = "sleepTime"
  val INGESTION_TYPE = "ingestionType"
  val KEY_INPUT_AVAIL_WAIT="waitForInputAvailability"
  val KEY_OUTPUT_CHECK="skipOutputCheck"
  val KEY_JOB_TRACKER = "jobTracker"
  val KEY_NAME_NODE = "nameNode"
  val KEY_HDFS_PROPS_DIR = "hdfsPropertiesDir"
  val KEY_HDFS_CONFIG_DIR = "hdfsConfigDir"
  val SFTP_TIME_FORMAT = "yyyyMMddHHmm"
  
  //Task Properties
  val HADOOP_FS_DEFAULT_NAME = "fs.default.name"

  val JSON_SNAPSHOT_KEY = "snapshot"
  val JSON_VIEWS_KEY = "views"
  val JSON_VIEWNAME = "viewName"
  val JSON_CLIENTS = "clientList"
  val JSON_CRON_EXPRESSION = "cronExpression"
  val JSON_JOB_START_TIME = "jobStartTime"
  val JSON_JOB_END_TIME = "jobEndTime"
  val JSON_JOB_COMPLEXITY = "complexity"
  val JSON_USERNAME = "userName"
  val CATALOG_DATE_FORMAT = "dd/MM/yyyy HH:mm"
  val TIMEZONE_UTC = "UTC"
  val OOZIE_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm'Z'"
  val OOZIE_NOMINAL_DATE_FORMAT = "yyyy-MM-dd'T'HH:mmZ"
  val SPARTAN_DATE_FORMAT = "yyyyMMddHHmm"

  val SAMPLE_SOURCE_LOC = "sample.catalog.file"
  val RUN_ENV = "env"
  val DEV_MODE = "dev"

  //http codes
  val HTTP_400 = 400
  val HTTP_500 = 500
  val HTTP_200 = 200

  //Http headers
  val HTTP_APPLICATION_JSON = "application/json"

  val OOZIE_RESPONSE_TIME_FORMAT = "EEE, dd MMM yyyy HH:mm:ss z"

  val CONTENT_TYPE = "application/json"
  val ENCODING = "UTF-8"
  val HTTP_TIMEOUT = 10000


  //Terminators
  val DEFAULT_TERMINATOR_INITIAL_DELAY = 0
  val DEFAULT_TERMINATOR_INTERVAL = 120

  val DEFAULT_OOZIE_URL = "http://localhost:11000/oozie/v1/job/"
  val DEFAULT_YARN_URL = "http://localhost:8088/ws/v1/cluster/apps"

  val RUNNING_FILTER_FILE = "runningFilter.json"
  val DEFAULT_ES_URL = "localhost:9200"

  val JOB_INDEX: String = "dp2-jobmetrics*/_search"

  val DEFAULT_SLA_TIMEOUT = 300
  val DEFAULT_SLA_MULTIPLIER = 2

  val SLA_TERMINATOR_INTERVAL_KEY = "sla.terminator.interval"

  val RETRY_INTERVAL ="retry.interval"
  var DEFAULT_RETRY_INTERVAL = 60
  val RETRY_WINDOW = "retry.window"
  val DEFAULT_RETRY_WINDOW = 600

  val EXPORTER_JOB_INTERVAL ="exporter.interval"
  var DEFAULT_EXPORTER_JOB_INTERVAL = 300

  val RETRYABLE_FILTER_FILE = "retryableFilter.json"

  val RETRY_FILTER_FETCHSIZE_KEY = "retry.filter.fetchsize"
  val DEFAULT_RETRY_FILTER_FETCHSIZE = 1000

  //dummy JDBC Connection
  val JDBC_DRIVER = "org.h2.Driver"
  val DB_URL = "jdbc:h2:mem:dummy"
  val USER = "sa"
  val PASS = ""

  //Kafka Event Source
  val EVENT_SOURCE = "dp2-jobmetrics"

  val EXTERNAL_ID = "externalId"
  val JOB_METRICS_QUERY_FILE = "jobMetrics.json"
  val HINGE_METRICS_QUERY_FILE = "hingeMetrics.json"
  val AUTOREPLAY_JOB_INTERVAL ="autoreplay.job.intervalInSeconds"
  var DEFAULT_AUTOREPLAY_JOB_INTERVAL_SECONDS = 43200

  val AUTO_REPLAY_THRESHOLD_PROPERTY  = "autoreplay.threshold.percentage"
  val AUTO_REPLAY_THRESHOLD_PERCENT_DEFAULT  = 10

  val AUTO_REPLAY_WINDOW_PROPERTY  = "autoreplay.window.durationInDays"
  val AUTO_REPLAY_WINDOW_DAYS_DEFAULT  = 3
}
