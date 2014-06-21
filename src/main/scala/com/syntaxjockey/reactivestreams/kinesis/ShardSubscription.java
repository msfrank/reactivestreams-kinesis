package com.syntaxjockey.reactivestreams.kinesis;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.*;
import org.reactivestreams.spi.Subscriber;
import org.reactivestreams.spi.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.*;

/**
 *
 */
public class ShardSubscription implements Subscription {

    private final Logger logger = LoggerFactory.getLogger(ShardSubscription.class);

    /* config */
    private final Subscriber<Record> subscriber;
    private final AmazonKinesisAsyncClient asyncClient;
    private final ScheduledExecutorService scheduledExecutorService;
    private final String streamName;
    private final String shardId;
    private final ShardIteratorType shardIteratorType;
    private final String startingSequenceNumber;
    private final Integer queuedMaximum = 100;
    private final long initialDelay = 1;
    private final long nextTickPeriod = 5;

    /* state */
    private final Set<Subscriber<Record>> subscriberSet;
    private Integer numRequested = 0;
    private LinkedList<Record> queued = new LinkedList<>();
    private Integer elementsLeft = -1;
    private String shardIterator = null;
    private GetShardIteratorHandler getShardIteratorHandler = new GetShardIteratorHandler();
    private Future<GetShardIteratorResult> getShardIteratorResultFuture = null;
    private GetRecordsHandler getRecordsHandler = new GetRecordsHandler();
    private Future<GetRecordsResult> getRecordsResultFuture = null;
    private ScheduledFuture nextTickResultFuture = null;
    private Throwable lastError = null;

    /**
     *
     */
    public ShardSubscription(Subscriber<Record> subscriber,
                             Set<Subscriber<Record>> subscriberSet,
                             AmazonKinesisAsyncClient asyncClient,
                             ScheduledExecutorService scheduledExecutorService,
                             String streamName,
                             String shardId,
                             ShardIteratorType shardIteratorType,
                             String startingSequenceNumber,
                             Integer maxElements) {
        /* set config */
        this.subscriber = subscriber;
        this.subscriberSet = subscriberSet;
        this.asyncClient = asyncClient;
        this.scheduledExecutorService = scheduledExecutorService;
        this.streamName = streamName;
        this.shardId = shardId;
        this.shardIteratorType = shardIteratorType;
        this.startingSequenceNumber = startingSequenceNumber;
        /* set state */
        if (maxElements > 0)
            this.elementsLeft = maxElements;
        /* */
        initialize();
    }

    /**
     *
     */
    private synchronized void initialize() {
        /* if no shard iterator is set, then retrieve one asynchronously */
        if (shardIterator == null && getShardIteratorResultFuture == null) {
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            getShardIteratorRequest.setStreamName(streamName);
            getShardIteratorRequest.setShardId(shardId);
            getShardIteratorRequest.setShardIteratorType(shardIteratorType);
            getShardIteratorRequest.setStartingSequenceNumber(startingSequenceNumber);
            this.getShardIteratorResultFuture = asyncClient.getShardIteratorAsync(getShardIteratorRequest, getShardIteratorHandler);
        }
    }

    /**
     *
     * @param currentIterator
     */
    private synchronized void start(String currentIterator) {
        shardIterator = currentIterator;
        nextTickResultFuture = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.debug("TICK");
                pull();
            }
        }, initialDelay, nextTickPeriod, TimeUnit.SECONDS);
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
        return lastError != null;
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
        numRequested = 0;
        queued = null;
        shardIterator = null;
        subscriberSet.remove(subscriber);
    }

    /**
     *
     * @param exception
     */
    private synchronized void error(Throwable exception) {
        lastError = exception;
        subscriber.onError(exception);
        shutdown();
    }

    /**
     *
     */
    private class GetShardIteratorHandler implements AsyncHandler<GetShardIteratorRequest,GetShardIteratorResult> {

        @Override
        public void onSuccess(GetShardIteratorRequest request, GetShardIteratorResult getShardIteratorResult) {
            logger.debug("GetShardIterator request returned success");
            start(getShardIteratorResult.getShardIterator());
        }

        @Override
        public void onError(Exception exception) {
            logger.error("GetShardIterator request returned error: {}", exception.getMessage());
            error(exception);
        }
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
            error(exception);
        }
    }
}
