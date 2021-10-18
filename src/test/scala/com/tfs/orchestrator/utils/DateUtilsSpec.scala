package com.tfs.orchestrator.utils

import java.util.Date

import org.scalatest.FlatSpec

class DateUtilsSpec extends FlatSpec {
  behavior of "DateUtils"

  val customFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"

  it should "return a date string, given epoch time, formatted and in UTC timezome" in {
    val dateString = DateUtils.epochToUTCString(customFormat, 1514764800000L)
    assert(dateString.equals("2018-01-01T00:00:00.000"))
  }


  it should "return a date string, given date, formatted and in UTC timezome" in {
    val dateString = DateUtils.dateToUTCString(customFormat, new Date(1514764800000L))
    assert(dateString.equals("2018-01-01T00:00:00.000"))
  }

  it should "return the date given epoch millis parsing it in UTC time zone" in {
    val date = DateUtils.utcStringToDate(customFormat, "2018-01-01T00:00:00.000")
    assert(date.equals(new Date(1514764800000L)))
  }
}
