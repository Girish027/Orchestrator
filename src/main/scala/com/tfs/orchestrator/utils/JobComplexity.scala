package com.tfs.orchestrator.utils

/**
 * Resources requirement is expressed in terms of degree. The enum provides the options
 */
object RESOURCE_DEGREE extends Enumeration {
    type RESOURCE_DEGREE = Value
    val LOW = Value("low")
    val MEDIUM = Value("medium")
    val HIGH = Value("high")
}
