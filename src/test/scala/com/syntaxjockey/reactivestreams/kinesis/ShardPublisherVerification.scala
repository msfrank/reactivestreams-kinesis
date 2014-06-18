package com.syntaxjockey.reactivestreams.kinesis

import org.scalatest.Assertions
import org.reactivestreams.spi.Publisher
import org.reactivestreams.tck.{TestEnvironment, PublisherVerification}
import com.amazonaws.services.kinesis.model.Record

//class ShardPublisherVerification(env: TestEnvironment, shutdownTimeout: Long) extends PublisherVerification(env, shutdownTimeout) with Assertions {
//
//  def this() = this(new TestEnvironment(10000), 10000)
//
//  override def createPublisher(elements: Int): Publisher[Record] = {
//    null
//  }
//
//  override def createCompletedStatePublisher(): Publisher[Record] = null
//
//  override def createErrorStatePublisher(): Publisher[Record] = null
//}
