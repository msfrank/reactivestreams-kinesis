package com.syntaxjockey.reactivestreams.kinesis;

import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.reactivestreams.api.Consumer;
import org.reactivestreams.api.Producer;
import org.reactivestreams.spi.Publisher;
import org.reactivestreams.spi.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

/**
 *
 */
public class ShardProducer implements Producer<Record> {

    private final Logger logger = LoggerFactory.getLogger(ShardProducer.class);

    private final ShardPublisher publisher;

    /**
     *
     * @param asyncClient
     * @param shardIterator
     * @param scheduledExecutorService
     */
    public ShardProducer(AmazonKinesisAsyncClient asyncClient, String shardIterator, ScheduledExecutorService scheduledExecutorService) {
        publisher = new ShardPublisher(asyncClient, shardIterator, scheduledExecutorService);
        logger.debug("initialized ShardProducer for shardIterator {}", shardIterator);
    }

    /**
     * Get the underlying {@link org.reactivestreams.spi.Publisher Publisher} for this
     * Producer. This method should only be used by implementations of this API.
     *
     * @return the underlying publisher for this producer
     */
    @Override
    public Publisher<Record> getPublisher() {
        return publisher;
    }

    /**
     * Connect the given consumer to this producer. This means that the Subscriber
     * underlying the {@link org.reactivestreams.api.Consumer Consumer} subscribes to
     * this Producer’s underlying {@link org.reactivestreams.spi.Publisher Publisher},
     * which will initiate the transfer of the produced stream of elements from
     * producer to consumer until either of three things happen: 1) the stream ends
     * normally (no more elements available); 2) the producer encounters a fatal error
     * condition; 3) the consumer cancels the reception of more elements.
     *
     * @param consumer The consumer to register with this producer.
     */
    @Override
    public void produceTo(Consumer<Record> consumer) {
        Subscriber<Record> subscriber = consumer.getSubscriber();
        publisher.subscribe(subscriber);
        logger.debug("produceTo subscribes {} to {}", toString(), consumer.toString());
    }

    /**
     *
     * @param asyncClient
     * @param getShardIteratorRequest
     * @param scheduledExecutorService
     * @return
     */
    static ShardProducer fromStream(AmazonKinesisAsyncClient asyncClient,
                                    GetShardIteratorRequest getShardIteratorRequest,
                                    ScheduledExecutorService scheduledExecutorService) throws AmazonKinesisException {
        Logger logger = LoggerFactory.getLogger(ShardProducer.class);
        try {
            logger.debug("building ShardProducer for stream {}, shard {}",
                    getShardIteratorRequest.getStreamName(),
                    getShardIteratorRequest.getShardId());
            GetShardIteratorResult getShardIteratorResult = asyncClient.getShardIterator(getShardIteratorRequest);
            String shardIterator = getShardIteratorResult.getShardIterator();
            logger.debug("shard iterator is {}", shardIterator);
            return new ShardProducer(asyncClient, shardIterator, scheduledExecutorService);
        } catch (Exception exception) {
            throw new AmazonKinesisException(exception);
        }
    }

    /**
     *
     * @param asyncClient
     * @param streamName
     * @param shardId
     * @param shardIteratorType
     * @param scheduledExecutorService
     * @return
     */
    static ShardProducer fromStream(AmazonKinesisAsyncClient asyncClient,
                                    String streamName,
                                    String shardId,
                                    ShardIteratorType shardIteratorType,
                                    ScheduledExecutorService scheduledExecutorService) throws AmazonKinesisException {
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                .withStreamName(streamName)
                .withShardId(shardId)
                .withShardIteratorType(shardIteratorType);
        return fromStream(asyncClient, getShardIteratorRequest, scheduledExecutorService);
    }

    /**
     *
     * @param asyncClient
     * @param streamName
     * @param shardId
     * @param shardIteratorType
     * @param startingSequenceNumber
     * @param scheduledExecutorService
     * @return
     */
    static ShardProducer fromStream(AmazonKinesisAsyncClient asyncClient,
                                    String streamName,
                                    String shardId,
                                    ShardIteratorType shardIteratorType,
                                    String startingSequenceNumber,
                                    ScheduledExecutorService scheduledExecutorService) throws AmazonKinesisException {
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                .withStreamName(streamName)
                .withShardId(shardId)
                .withShardIteratorType(shardIteratorType)
                .withStartingSequenceNumber(startingSequenceNumber);
        return fromStream(asyncClient, getShardIteratorRequest, scheduledExecutorService);
    }
}
