package com.syntaxjockey.reactivestreams.kinesis;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.reactivestreams.api.Consumer;
import org.reactivestreams.api.Producer;
import org.reactivestreams.spi.Publisher;
import org.reactivestreams.spi.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

/**
 *
 */
public class ShardProducer implements Producer<Record> {

    private final Logger logger = LoggerFactory.getLogger(ShardProducer.class);

    private final ShardPublisher publisher;

    /**
     *
     */
    public ShardProducer(AmazonKinesisAsyncClient asyncClient,
                         ScheduledExecutorService scheduledExecutorService,
                         String streamName,
                         String shardId,
                         ShardIteratorType shardIteratorType,
                         String startingSequenceNumber,
                         Integer maxElements) {
        publisher = new ShardPublisher(asyncClient,
                                       scheduledExecutorService,
                                       streamName,
                                       shardId,
                                       shardIteratorType,
                                       startingSequenceNumber,
                                       maxElements);
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
        logger.debug("{} produces to {}", toString(), consumer.toString());
    }
}
