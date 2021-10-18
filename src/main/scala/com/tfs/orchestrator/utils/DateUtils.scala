package com.tfs.orchestrator.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar, TimeZone}


/**
 * Common util functions for UTC timezone based conversions
 */
object DateUtils {

  def epochToUTCString(format: String, timeInMillis: Long): String = {
    val dateFormatter = new SimpleDateFormat(format)
    dateFormatter.setTimeZone(TimeZone.getTimeZone(Constants.TIMEZONE_UTC))
    dateFormatter.format(new Date(timeInMillis))
  }

  def dateToUTCString(format: String, date: Date): String = {
    val dateFormatter = new SimpleDateFormat(format)
    dateFormatter.setTimeZone(TimeZone.getTimeZone(Constants.TIMEZONE_UTC))
    dateFormatter.format(date)
  }

  def utcStringToDate(format: String, timeString: String): Date = {
    val dateFormatter = new SimpleDateFormat(format)
    dateFormatter.setTimeZone(TimeZone.getTimeZone(Constants.TIMEZONE_UTC))
    dateFormatter.parse(timeString)
  }

  def convertToNextHourDate(date: Date):Date ={
    val gc = new GregorianCalendar()
    gc.setTime(date)
    gc.add(Calendar.HOUR,1)
    gc.set(Calendar.MINUTE,0)
    gc.set(Calendar.SECOND,0)
    gc.getTime
  }

  def utcStringToEpoch(format: String, timeString : String): Long ={
    val dateFormatter = new SimpleDateFormat(format)
    val date = dateFormatter.parse(timeString)
    val cal = Calendar.getInstance();
    cal.setTime(date);
    cal.getTimeInMillis
  }

  def fetchClientZoneTime(epoch:Long, timezone:String, format:String):String={
    val dateFormat = new SimpleDateFormat(format) 
    dateFormat.setTimeZone(TimeZone.getTimeZone(timezone))
    dateFormat.format(epoch)
  }
}
