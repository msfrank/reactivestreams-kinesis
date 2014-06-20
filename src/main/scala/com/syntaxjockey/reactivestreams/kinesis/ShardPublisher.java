package com.syntaxjockey.reactivestreams.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.reactivestreams.spi.Publisher;
import org.reactivestreams.spi.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 *
 */
public class ShardPublisher implements Publisher<Record> {

    private final Logger logger = LoggerFactory.getLogger(ShardPublisher.class);

    /* config */
    private final AmazonKinesisAsyncClient asyncClient;
    private final ScheduledExecutorService scheduledExecutorService;
    private final String streamName;
    private final String shardId;
    private final ShardIteratorType shardIteratorType;
    private final String startingSequenceNumber;
    private final Integer maxElements;

    /* state */
    private Subscriber<Record> subscriber = null;

    /**
     *
     */
    public ShardPublisher(AmazonKinesisAsyncClient asyncClient,
                          ScheduledExecutorService scheduledExecutorService,
                          String streamName,
                          String shardId,
                          ShardIteratorType shardIteratorType,
                          String startingSequenceNumber,
                          Integer maxElements) {
        this.asyncClient = asyncClient;
        this.scheduledExecutorService = scheduledExecutorService;
        this.streamName = streamName;
        this.shardId = shardId;
        this.shardIteratorType = shardIteratorType;
        this.startingSequenceNumber = startingSequenceNumber;
        this.maxElements = maxElements;
    }

    /**
     * Subscribe the given {@link org.reactivestreams.spi.Subscriber Subscriber} to this
     * Publisher. A Subscriber can at most be subscribed once to a given Publisher, and
     * to at most one Publisher in total.
     *
     * @param subscriber The subscriber to register with this publisher.
     */
    @Override
    public synchronized void subscribe(Subscriber<Record> subscriber) {
        if (this.subscriber == subscriber) {
            subscriber.onError(new IllegalStateException("ShardPublisher is already subscribed to"));
        }
        else if (this.subscriber != null) {
            subscriber.onError(new Exception("ShardPublisher is already subscribed to"));
        } else {
            this.subscriber = subscriber;
            ShardSubscription subscription = new ShardSubscription(subscriber,
                                                                   asyncClient,
                                                                   scheduledExecutorService,
                                                                   streamName,
                                                                   shardId,
                                                                   shardIteratorType,
                                                                   startingSequenceNumber,
                                                                   maxElements);
            subscriber.onSubscribe(subscription);
            logger.debug("subscribed {} to {}", subscriber.toString(), subscription.toString());
        }
    }
}
