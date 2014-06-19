package com.syntaxjockey.reactivestreams.kinesis

import org.scalatest.testng.TestNGSuite
import org.testng.annotations.{AfterSuite, BeforeSuite}
import org.reactivestreams.spi.Publisher
import org.reactivestreams.tck.{TestEnvironment, PublisherVerification}
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.amazonaws.services.kinesis.model._
import com.amazonaws.auth.PropertiesCredentials

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.ScheduledThreadPoolExecutor

class ShardPublisherVerification(streamName: String, env: TestEnvironment, shutdownTimeout: Long)
  extends PublisherVerification[com.amazonaws.services.kinesis.model.Record](env, shutdownTimeout) with TestNGSuite {

  val scheduler = new ScheduledThreadPoolExecutor(2)
  val credentials = new PropertiesCredentials(new File("aws.credentials"))
  val asyncClient = new AmazonKinesisAsyncClient(credentials)
  asyncClient.setEndpoint("kinesis.us-east-1.amazonaws.com", "kinesis", "us-east-1")

  var shard: Shard = null

  def this() = this("reactivestreams-kinesis-test", new TestEnvironment(10000), 10000)

  @BeforeSuite
  def beforeSuite(): Unit = {

    // pick the first shard we find
    val describeStreamRequest = new DescribeStreamRequest()
    describeStreamRequest.setStreamName(streamName)
    val describeStreamResult = asyncClient.describeStream(describeStreamRequest)
    if (describeStreamResult.getStreamDescription.getShards.size() != 1)
      throw new Exception("failed to initialize ShardPublisherVerification: stream must have only 1 shard")
    if (describeStreamResult.getStreamDescription.getHasMoreShards == true)
      throw new Exception("failed to initialize ShardPublisherVerification: stream must have only 1 shard")
    shard = describeStreamResult.getStreamDescription.getShards.get(0)

    // push elements into kinesis so we know we have some data
    for (j <- 0.until(10)) {
      val putRecordRequest = new PutRecordRequest()
      putRecordRequest.setStreamName(streamName)
      putRecordRequest.setData(ByteBuffer.wrap("testData-%d".format(j).getBytes))
      putRecordRequest.setPartitionKey("partitionKey")
      val putRecordResult = asyncClient.putRecord(putRecordRequest)
    }
  }

  override def createPublisher(elements: Int): Publisher[Record] = {
    val producer = ShardProducer.fromStream(asyncClient,
                             streamName,
                             shard.getShardId,
                             ShardIteratorType.TRIM_HORIZON,
                             new ScheduledThreadPoolExecutor(2),
                             if (elements == 0) -1 else elements)
    producer.getPublisher
  }

  override def createCompletedStatePublisher(): Publisher[Record] = null

  override def createErrorStatePublisher(): Publisher[Record] = null
}
