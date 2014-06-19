package com.syntaxjockey.reactivestreams.kinesis;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import org.reactivestreams.spi.Publisher;
import org.reactivestreams.spi.Subscriber;
import org.reactivestreams.spi.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

/**
 *
 */
public class ShardPublisher implements Publisher<Record>, Subscription {

    private final Logger logger = LoggerFactory.getLogger(ShardProducer.class);

    /* config */
    private final AmazonKinesisAsyncClient asyncClient;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Integer queuedMaximum = 100;
    private final long initialDelay = 1;
    private final long nextTickPeriod = 5;

    /* state */
    private Subscriber<Record> subscriber = null;
    private String shardIterator = null;
    private Integer numRequested = 0;
    private Integer elementsLeft = -1;
    private LinkedList<Record> queued = new LinkedList<>();
    private GetRecordsHandler getRecordsHandler = new GetRecordsHandler();
    private Future<GetRecordsResult> getRecordsResultFuture = null;
    private ScheduledFuture nextTickResultFuture = null;
    private Throwable error = null;

    /**
     *
     * @param asyncClient
     * @param shardIterator
     * @param scheduledExecutorService
     */
    public ShardPublisher(AmazonKinesisAsyncClient asyncClient,
                          String shardIterator,
                          ScheduledExecutorService scheduledExecutorService,
                          Integer maxElements) {
        this.asyncClient = asyncClient;
        this.scheduledExecutorService = scheduledExecutorService;
        this.shardIterator = shardIterator;
        this.elementsLeft = maxElements;
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
        if (this.subscriber == null) {
            this.subscriber = subscriber;
            subscriber.onSubscribe(this);
            nextTickResultFuture = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    logger.debug("TICK");
                    pull();
                }
            }, initialDelay, nextTickPeriod, TimeUnit.SECONDS);
            logger.debug("subscribed {} to {}", subscriber.toString(), toString());
        }
        else {
            subscriber.onError(new IllegalStateException("ShardPublisher is already subscribed to"));
        }
    }

    /**
     * Cancel this subscription. The {@link org.reactivestreams.spi.Publisher Publisher}
     * to which produced this Subscription will eventually stop sending more elements to
     * the {@link org.reactivestreams.spi.Subscriber Subscriber} which owns this Subscription.
     * This may happen before the requested number of elements has been delivered, even if
     * the Publisher would still have more elements.
     */
    @Override
    public synchronized void cancel() {
        logger.debug("cancelling subscription");
        subscriber.onComplete();
        shutdown();
    }

    /**
     * Request more data from the {@link org.reactivestreams.spi.Publisher Publisher}
     * which produced this Subscription. The number of requested elements is cumulative
     * to the number requested previously. The Publisher may eventually publish up to the
     * requested number of elements to the {@link org.reactivestreams.spi.Subscriber Subscriber}
     * which owns this Subscription.
     *
     * @param elements The number of elements requested.
     */
    @Override
    public synchronized void requestMore(int elements) {
        logger.debug("subscriber requests {} records", elements);
        numRequested += elements;
    }

    /**
     * Checks if publisher is in 'error' state.
     *
     * @return true if the publisher is in 'error' state, otherwise false.
     */
    public synchronized Boolean isError() {
        return error != null;
    }

    /**
     * Checks if publisher is in 'completed' state.
     *
     * @return true if the publisher is in 'completed' state, otherwise false.
     */
    public synchronized Boolean isCompleted() {
       return elementsLeft == 0;
    }

    /**
     *
     */
    private synchronized void pull() {
        if (getRecordsResultFuture == null) {
            Integer limit = numRequested + queuedMaximum - queued.size();
            if (limit > 0) {
                logger.debug("requesting {} records using shardIterator {}", limit, shardIterator);
                GetRecordsRequest request = new GetRecordsRequest()
                        .withShardIterator(shardIterator)
                        .withLimit(limit);
                getRecordsResultFuture = asyncClient.getRecordsAsync(request, getRecordsHandler);
            }
        }
    }

    /**
     *
     * @param getRecordsResult
     */
    private synchronized void push(GetRecordsResult getRecordsResult) {
        // invariant:
        assert(elementsLeft != 0);
        /* store the next shard iterator */
        shardIterator = getRecordsResult.getNextShardIterator();
        getRecordsResultFuture = null;
        /* append records to the queue */
        List<Record> records = getRecordsResult.getRecords();
        queued.addAll(records);
        logger.debug("received {} records", records.size());
        Integer numPushed = 0;
        try {
            /* notify subscriber of requested elements */
            while (numRequested > 0 && elementsLeft != 0) {
                Record record = queued.remove();    // may throw NoSuchElementException
                subscriber.onNext(record);
                numRequested--;
                numPushed++;
                if (elementsLeft > 0)   // decrement only if count is positive (negative means infinite stream)
                    elementsLeft--;
            }
            logger.debug("pushed {} records, {} records queued", numPushed, queued.size());
            /* if stream is finite, then check if we are complete */
            if (elementsLeft == 0) {
                subscriber.onComplete();
                shutdown();
            }
        } catch (NoSuchElementException e) {
            logger.debug("pushed {} records, {} requests outstanding ", numPushed, numRequested);
        }
    }

    /**
     * Only call this method within synchronized context.
     */
    private void shutdown() {
        if (nextTickResultFuture != null) {
            nextTickResultFuture.cancel(true);
            nextTickResultFuture = null;
        }
        if (getRecordsResultFuture != null) {
            getRecordsResultFuture.cancel(true);
            getRecordsResultFuture = null;
        }
        subscriber = null;
        numRequested = 0;
        queued = null;
    }

    /**
     *
     */
    private class GetRecordsHandler implements AsyncHandler<GetRecordsRequest,GetRecordsResult> {

        @Override
        public void onSuccess(GetRecordsRequest request, GetRecordsResult result) {
            logger.debug("GetRecords request returned success");
            push(result);
        }

        @Override
        public void onError(Exception exception) {
            logger.error("GetRecords request returned error: {}", exception.getMessage());
        }
    }
}
