package com.syntaxjockey.reactivestreams.kinesis

import java.io.File
import java.util.concurrent.ScheduledThreadPoolExecutor

import akka.actor.ActorSystem
import akka.stream.{FlowMaterializer, MaterializerSettings}
import akka.stream.scaladsl.Flow
import akka.testkit.{ImplicitSender, TestKit}
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.amazonaws.services.kinesis.model.ShardIteratorType
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._

//class ShardProducerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers {
//
//  val logger = LoggerFactory.getLogger(classOf[ShardProducerSpec])
//
//  def this() = this(ActorSystem("ShardProducerSpec"))
//
//  "A ShardProducer" must {
//
//    "read 3 elements from kinesis and print them" in {
//
//      // configure scheduler
//      val scheduler = new ScheduledThreadPoolExecutor(2)
//
//      // configure client
//      val credentials = new PropertiesCredentials(new File("aws.credentials"))
//      val client = new AmazonKinesisAsyncClient(credentials)
//      client.setEndpoint("kinesis.us-east-1.amazonaws.com", "kinesis", "us-east-1")
//
//      val kinesis = ShardProducer.fromStream(client, "reactivestreams-kinesis-test", "shardId-000000000000", ShardIteratorType.TRIM_HORIZON, scheduler, -1)
//
//      val future = Flow(kinesis).take(3).map { record =>
//        new String(record.getData.array())
//      }.foreach { record =>
//        logger.info("record: {}", record)
//      }.toFuture(FlowMaterializer(MaterializerSettings()))
//
//      val result = Await.ready(future, 30.seconds)
//
//      client.shutdown()
//      scheduler.shutdown()
//      system.shutdown()
//    }
//  }
//}
