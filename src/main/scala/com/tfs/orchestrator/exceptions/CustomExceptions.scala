package com.tfs.orchestrator.exceptions

case class InvalidParameterException(message: String) extends Exception(message)

case class RestClientException(message:String, cause: Throwable=null) extends Exception(message, cause)

case class InvalidJsonException(message: String) extends Exception(message)

case class HibernateException(cause: Throwable, msg: String) extends Exception(msg, cause) {
  def this(msg: String) {
    this(null, msg)
  }
}

case class ScheduleException(cause: Throwable, message:String) extends Exception(message, cause) {
  def this(msg: String) {
    this(null, msg)
  }
}

case class InitializationException(cause: Throwable, message:String) extends Exception(message, cause) {
  def this(msg: String) {
    this(null, msg)
  }
}

case class InvalidIngestionType(message:String) extends Exception(message) {}
