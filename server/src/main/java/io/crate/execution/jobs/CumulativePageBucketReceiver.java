/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.jobs;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;
import org.elasticsearch.common.util.concurrent.PriorityRunnable;

import io.crate.Streamer;
import io.crate.common.annotations.GuardedBy;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.distribution.merge.BatchPagingIterator;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.execution.engine.distribution.merge.PagingIterator;
import io.netty.util.collection.IntObjectHashMap;

/**
 * A {@link PageBucketReceiver} which receives buckets from upstreams, merges them into a
 * {@link PagingIterator} and feeds a single {@link RowConsumer}.
 *
 * <h2>How much of a page is needed</h2>
 * If the paging iterator establishes an order across buckets
 * ({@link PagingIterator#requiresAllBucketsPerPage()}), a page is only merged once every upstream
 * delivered its bucket -- no row can be emitted before the head of each bucket is known. A
 * pass-through merge has no such requirement, so each bucket is merged as it arrives and its upstream
 * is acknowledged immediately.
 *
 * <p>That difference matters beyond latency: withholding the acknowledgement until every upstream of a
 * page delivered can deadlock a chain of distributed joins, because an upstream that filled its own
 * output page stops consuming its input until its downstreams acknowledge. See
 * {@code CumulativePageBucketReceiverTest#test_unordered_merge_acknowledges_upstream_without_waiting_for_the_others}.
 *
 * <h2>Three states, deliberately separate</h2>
 * Merging buckets independently means these stop coinciding, and conflating any two of them causes
 * either lost rows or a hang:
 * <ol>
 *   <li><b>upstreams exhausted</b> -- every upstream sent its last bucket ({@link #exhausted}). Says
 *       nothing about whether those buckets reached the consumer.</li>
 *   <li><b>pages in flight</b> -- produced but not yet delivered ({@link #pagesPendingForConsumer}).
 *       A page is produced under {@link #lock} and dispatched to {@link #executor} only afterwards, so
 *       in between it is in neither {@link #readyPages} nor the consumer.</li>
 *   <li><b>consumer finished</b> -- {@link #finishedForConsumer}, latched by {@link #fetchMore} once
 *       the first two say there is nothing left. Only this one may be reported to the consumer as
 *       {@code allLoaded()}.</li>
 * </ol>
 */
public class CumulativePageBucketReceiver implements PageBucketReceiver {

    private static final Logger LOGGER = LogManager.getLogger(CumulativePageBucketReceiver.class);

    /**
     * Priority to finish queries is higher than starting new queries, except for system queries.
     * See also `getPriority` in {@link CollectTask}
     **/
    private static final Priority PRIORITY = Priority.HIGH;

    private final Object lock = new Object();
    private final String nodeName;
    private final boolean traceEnabled;
    private final int phaseId;
    private final Executor executor;
    private final Streamer<?>[] streamers;
    private final int numBuckets;
    @GuardedBy("lock")
    private final Set<Integer> exhausted;
    private final Map<Integer, PageResultListener> listenersByBucketIdx;
    @GuardedBy("lock")
    private final Map<Integer, Bucket> bucketsByIdx;
    private final RowConsumer consumer;
    private final PagingIterator<Integer, Row> pagingIterator;
    /**
     * Whether a page must be complete before it is merged. See
     * {@link PagingIterator#requiresAllBucketsPerPage()}.
     * <p>
     * Also true for a single upstream, where it holds trivially: a page is complete as soon as that
     * upstream's bucket arrives, so there is nothing to hand over independently and no cycle to
     * break -- the deadlock this receiver guards against needs at least two upstreams. Saying so here
     * keeps the degenerate case on exactly the same path it took before independent hand-over
     * existed, instead of routing it through machinery that cannot help it.
     */
    private final boolean requiresAllBucketsPerPage;
    private final BatchIterator<Row> batchPagingIterator;
    private final CompletableFuture<?> processingFuture = new CompletableFuture<>();

    /**
     * Volatile because {@link #setBucket} reads it outside of {@link #lock} to answer a bucket that
     * arrives after a failure with {@code needMore(false)}. Without that visibility guarantee such an
     * upstream would instead be left to discover the dead receiver via the retry/broadcast-kill path
     * in {@code TransportDistributedResultAction}.
     */
    private volatile Throwable lastThrowable = null;

    /**
     * Pages that are complete but for which no {@link #fetchMore} call is currently waiting.
     * <p>
     * While a page is only handed on once buckets from all upstreams arrived, this can hold at most
     * one entry. It exists so that a page becoming ready and a {@code fetchMore} asking for one are
     * decoupled: previously a single {@code CompletableFuture} was used for both, so a page
     * completed while nothing was waiting was silently lost when the next {@code fetchMore} replaced
     * the future.
     */
    @GuardedBy("lock")
    private final ArrayDeque<List<KeyIterable<Integer, Row>>> readyPages = new ArrayDeque<>();

    /** A {@link #fetchMore} call waiting for the next page, or {@code null} if none is waiting. */
    @GuardedBy("lock")
    @Nullable
    private CompletableFuture<List<KeyIterable<Integer, Row>>> pageRequest;

    /**
     * Number of pages that have been produced by {@link #processPage} but not yet handed to the
     * consumer.
     * <p>
     * {@link #readyPages} alone cannot express this: a page is produced under {@link #lock} and only
     * afterwards dispatched to {@link #executor}, so between those two points it is in flight and
     * invisible to the queue. Treating the queue as authoritative lets the consumer conclude it is
     * finished while a page still holds rows, which silently truncates the result.
     */
    @GuardedBy("lock")
    private int pagesPendingForConsumer;

    /**
     * Latched completion signal for the consumer, set only by {@link #fetchMore} once it establishes
     * that nothing is left.
     * <p>
     * {@link BatchIterator} requires {@code loadNextBatch()} to throw when called after
     * {@code allLoaded()}, and its consumers check the two in sequence. Deriving completion directly
     * from upstream state would let it turn true between those two steps -- an arriving bucket or a
     * page hand-over on another thread -- and the consumer would trip that guard through no fault of
     * its own. Latching it inside {@code fetchMore} means it only ever changes on the consumer's own
     * thread, as a result of the consumer asking.
     */
    private volatile boolean finishedForConsumer = false;

    /**
     * Whether a consumer has been started on {@link #batchPagingIterator}.
     * <p>
     * Exactly one may ever be started: the first page to arrive starts it, or {@link #consumeRows()}
     * does, or {@link #kill} activates it with a failure. Since buckets are handed over independently,
     * several of them can reach {@link #triggerConsumerOrPageFuture} concurrently, and a second
     * consumer racing the first on the paging iterator loses rows.
     */
    @GuardedBy("lock")
    private boolean consumerStarted = false;

    public CumulativePageBucketReceiver(String nodeName,
                                        int phaseId,
                                        Executor executor,
                                        Streamer<?>[] streamers,
                                        RowConsumer rowConsumer,
                                        PagingIterator<Integer, Row> pagingIterator,
                                        int numBuckets) {
        this.nodeName = nodeName;
        this.phaseId = phaseId;
        this.executor = executor;
        this.streamers = streamers;
        this.consumer = rowConsumer;
        this.pagingIterator = pagingIterator;
        this.requiresAllBucketsPerPage = pagingIterator.requiresAllBucketsPerPage() || numBuckets <= 1;
        this.numBuckets = numBuckets;

        this.exhausted = Collections.newSetFromMap(new IntObjectHashMap<>(numBuckets));
        this.bucketsByIdx = new IntObjectHashMap<>(numBuckets);
        this.listenersByBucketIdx = new IntObjectHashMap<>(numBuckets);
        processingFuture.whenComplete((result, ex) -> {
            synchronized (listenersByBucketIdx) {
                for (PageResultListener resultListener : listenersByBucketIdx.values()) {
                    resultListener.needMore(false);
                }
                listenersByBucketIdx.clear();
            }
        });
        batchPagingIterator = new BatchPagingIterator<>(
            pagingIterator,
            this::fetchMore,
            this::isFinishedForConsumer,
            throwable -> {
                if (throwable == null) {
                    processingFuture.complete(null);
                } else {
                    processingFuture.completeExceptionally(throwable);
                }
            }
        );
        traceEnabled = LOGGER.isTraceEnabled();
    }

    @Override
    public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
        final boolean isLastOrHasError;
        final boolean duplicateBucket;
        synchronized (listenersByBucketIdx) {
            isLastOrHasError = isLast || lastThrowable != null;
            if (isLastOrHasError) {
                duplicateBucket = false;
            } else {
                // A still registered listener means this upstream has not been acknowledged yet, so a
                // second page from it violates the protocol. This is the only way to detect it when
                // buckets are processed independently, because getBuckets() then removes the bucket
                // from bucketsByIdx right away and the check below can no longer fire.
                duplicateBucket = listenersByBucketIdx.put(bucketIdx, pageResultListener) != null;
            }
        }
        if (duplicateBucket) {
            processingFuture.completeExceptionally(new IllegalStateException(String.format(Locale.ENGLISH,
                "Same bucket of a page set more than once. node=%s method=setBucket phaseId=%d bucket=%d",
                nodeName, phaseId, bucketIdx)));
            return;
        }
        if (isLastOrHasError) {
            pageResultListener.needMore(false);
        }
        final boolean allBucketsOfPageReceived;
        synchronized (lock) {
            if (traceEnabled) {
                LOGGER.trace("method=setBucket phaseId={} bucket={} size={} istLast={}", phaseId, bucketIdx, rows.size(), isLast);
            }

            if (bucketsByIdx.putIfAbsent(bucketIdx, rows) != null) {
                processingFuture.completeExceptionally(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Same bucket of a page set more than once. node=%s method=setBucket phaseId=%d bucket=%d",
                    nodeName, phaseId, bucketIdx)));
            }
            if (isLast) {
                exhausted.add(bucketIdx);
            }
            // A pass-through merge does not establish an order across buckets, so a bucket can be
            // handed on as soon as it arrives instead of waiting for every upstream of the page. That
            // decoupling is what lets an upstream be acknowledged while a sibling upstream is still
            // busy, so a chain of distributed operators cannot deadlock on the page barrier.
            allBucketsOfPageReceived = requiresAllBucketsPerPage == false
                                       || bucketsByIdx.size() == numBuckets;
            if (allBucketsOfPageReceived) {
                // Count the page here, in the same critical section that records the exhaustion of
                // this upstream. Counting it later, in processPage, leaves a window in which the
                // consumer sees every upstream exhausted with nothing pending, concludes that it is
                // done, and stops before the page it is about to produce ever reaches it.
                pagesPendingForConsumer++;
            }
        }
        if (allBucketsOfPageReceived) {
            processPage();
        }
    }

    private void triggerConsumerOrPageFuture(List<KeyIterable<Integer, Row>> buckets) {
        boolean invokeConsumer = false;
        Throwable throwable;
        synchronized (lock) {
            if (consumerStarted == false) {
                consumerStarted = true;
                invokeConsumer = true;
            }
            throwable = lastThrowable;
        }
        if (invokeConsumer) {
            pageHandedToConsumer(); // merged right below, or the whole operation fails
            if (throwable == null) {
                try {
                    pagingIterator.merge(buckets);
                    PrioritizedRunnable runnable = PriorityRunnable.of(
                        PRIORITY,
                        "pageBucketReceiver",
                        this::consumeRows
                    );
                    executor.execute(runnable);
                } catch (Throwable e) {
                    consumer.accept(null, e);
                    throwable = e;
                }
            } else {
                consumer.accept(null, throwable);
            }
        } else {
            if (throwable == null) {
                try {
                    PrioritizedRunnable runnable = PriorityRunnable.of(
                        PRIORITY,
                        "pageBucketReceiver",
                        () -> offerPage(buckets)
                    );
                    executor.execute(runnable);
                } catch (RejectedExecutionException e) {
                    failPageRequest(e);
                    throwable = e;
                }
            } else {
                failPageRequest(throwable);
            }
        }
        if (throwable != null) {
            processingFuture.completeExceptionally(throwable);
        }
    }

    private void processPage() {
        List<KeyIterable<Integer, Row>> buckets;
        try {
            buckets = getBuckets();
        } catch (Throwable t) {
            kill(t); // Also takes care of failing a waiting page request.
            return;
        }
        if (allUpstreamsExhausted()) {
            // Deliberately the loose check: when buckets are processed independently this can run
            // while a page is still queued in readyPages, so a merge may follow this finish() call.
            // That is only safe because the unordered iterator's finish() is a no-op; the ordered
            // ones, for which finish() is meaningful, keep the all-buckets barrier and therefore
            // reach this only after the final merge. An unordered iterator with a meaningful
            // finish() would have to defer this until isFinishedForConsumer() holds.
            pagingIterator.finish();
        }
        // The page was already counted in setBucket, atomically with recording this upstream's
        // exhaustion. See pagesPendingForConsumer.
        triggerConsumerOrPageFuture(buckets);
    }

    /**
     * A page reached the consumer. Balances the single increment in {@link #setBucket}, and is the only
     * way the count goes down apart from {@link #failPageRequest}, so the balance can be audited from
     * these call sites alone.
     * <p>
     * {@code synchronized} is reentrant, so this may be called while already holding {@link #lock}.
     */
    private void pageHandedToConsumer() {
        synchronized (lock) {
            pagesPendingForConsumer--;
            assert pagesPendingForConsumer >= 0 : "more pages handed to the consumer than produced";
        }
    }

    private List<KeyIterable<Integer, Row>> getBuckets() {
        List<KeyIterable<Integer, Row>> buckets = new ArrayList<>(numBuckets);
        synchronized (lock) {
            Iterator<Map.Entry<Integer, Bucket>> entryIt = bucketsByIdx.entrySet().iterator();
            while (entryIt.hasNext()) {
                Map.Entry<Integer, Bucket> entry = entryIt.next();
                Integer bucketIdx = entry.getKey();
                buckets.add(new KeyIterable<>(bucketIdx, entry.getValue()));
                if (exhausted.contains(bucketIdx)) {
                    entry.setValue(Bucket.EMPTY);
                } else {
                    entryIt.remove();
                }
            }
        }
        return buckets;
    }

    private boolean allUpstreamsExhausted() {
        return exhausted.size() == numBuckets;
    }

    /**
     * Whether the consumer may treat the source as finished.
     * <p>
     * Stricter than {@link #allUpstreamsExhausted()}: every produced page must also have reached the
     * consumer. When buckets are processed independently a page can be produced *after* the last
     * upstream became exhausted, so exhaustion alone no longer implies that everything has been
     * merged into the paging iterator. Reporting the source as finished at that point makes the
     * consumer stop without fetching the outstanding page, silently dropping its rows.
     * <p>
     * Counts rather than {@link #readyPages} emptiness, because a page is produced under
     * {@link #lock} and dispatched to {@link #executor} only afterwards -- in between it belongs to
     * neither.
     */
    private boolean isFinishedForConsumer() {
        if (requiresAllBucketsPerPage) {
            // Unchanged from the barrier behaviour: a page only becomes available once every bucket of
            // it arrived, so exhaustion coincides with everything having been merged.
            return allUpstreamsExhausted();
        }
        return finishedForConsumer;
    }

    private CompletionStage<? extends Iterable<? extends KeyIterable<Integer, Row>>> fetchMore(Integer exhaustedBucket) {
        CompletableFuture<List<KeyIterable<Integer, Row>>> request;
        List<KeyIterable<Integer, Row>> alreadyReady;
        boolean waitForPendingPage = false;
        synchronized (lock) {
            // Check for a queued page before testing for exhaustion: when buckets are processed
            // independently, the last upstream can become exhausted while a page of another upstream
            // is still queued here.
            alreadyReady = readyPages.poll();
            if (alreadyReady != null) {
                pageHandedToConsumer();
                request = null;
            } else if (pagesPendingForConsumer > 0) {
                // A page has been produced but not dispatched to the executor yet. Waiting for it is
                // essential rather than merely correct: reporting exhaustion here would make the
                // consumer spin on empty pages and, since production is dispatched to the same
                // executor, starve the very task that has to deliver the page.
                assert pageRequest == null : "only one page may be requested at a time";
                request = new CompletableFuture<>();
                pageRequest = request;
                waitForPendingPage = true;
            } else if (allUpstreamsExhausted()) {
                if (requiresAllBucketsPerPage) {
                    return CompletableFuture.failedStage(
                        new IllegalStateException("Source is exhausted"));
                }
                // Not an error here: the consumer can drain the buckets it already got and ask for
                // more while the last upstream's final bucket is still on its way. By now everything
                // produced has reached the consumer and processPage() has called
                // pagingIterator.finish(), so an empty page lets the iterator observe that it is done
                // instead of failing the query.
                finishedForConsumer = true;
                return CompletableFuture.completedFuture(List.of());
            } else {
                assert pageRequest == null : "only one page may be requested at a time";
                request = new CompletableFuture<>();
                pageRequest = request;
            }
        }
        if (alreadyReady != null) {
            // The upstreams that contributed this page were already asked for it, so no new request
            // is sent here.
            return CompletableFuture.completedFuture(alreadyReady);
        }
        if (waitForPendingPage) {
            // The page is already on its way; asking upstreams for another one would request data
            // nobody is waiting for.
            return request;
        }
        if (exhaustedBucket == null || exhausted.contains(exhaustedBucket)) {
            fetchFromUnExhausted();
        } else {
            fetchExhausted(exhaustedBucket);
        }
        return request;
    }

    /**
     * Hands a completed page to a waiting {@link #fetchMore}, or queues it until one arrives.
     */
    private void offerPage(List<KeyIterable<Integer, Row>> buckets) {
        CompletableFuture<List<KeyIterable<Integer, Row>>> waiting;
        synchronized (lock) {
            waiting = pageRequest;
            pageRequest = null;
            if (waiting == null) {
                // Stays counted in pagesPendingForConsumer until fetchMore takes it off the queue.
                readyPages.add(buckets);
            } else {
                pageHandedToConsumer();
            }
        }
        if (waiting != null) {
            waiting.complete(buckets);
        }
    }

    private void failPageRequest(Throwable t) {
        CompletableFuture<List<KeyIterable<Integer, Row>>> waiting;
        synchronized (lock) {
            waiting = pageRequest;
            pageRequest = null;
            readyPages.clear();
            // Nothing will be handed to the consumer any more; leaving these counted would make
            // isFinishedForConsumer() never report completion.
            pagesPendingForConsumer = 0;
        }
        if (waiting != null) {
            waiting.completeExceptionally(t);
        }
    }

    private void fetchExhausted(Integer exhaustedBucket) {
        synchronized (listenersByBucketIdx) {
            PageResultListener pageResultListener = listenersByBucketIdx.remove(exhaustedBucket);
            if (requiresAllBucketsPerPage) {
                // We're only requesting data for 1 specific bucket,
                // so we need to fill in other buckets to meet the
                // "receivedAllBucketsOfPage" condition once we get the data for this bucket
                for (Integer bucketIdx : listenersByBucketIdx.keySet()) {
                    bucketsByIdx.putIfAbsent(bucketIdx, Bucket.EMPTY);
                }
            }
            pageResultListener.needMore(true);
        }
    }

    private void fetchFromUnExhausted() {
        synchronized (listenersByBucketIdx) {
            for (PageResultListener listener : listenersByBucketIdx.values()) {
                listener.needMore(true);
            }
            listenersByBucketIdx.clear();
        }
    }

    @Override
    public Streamer<?>[] streamers() {
        return streamers;
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return processingFuture;
    }

    @Override
    public void consumeRows() {
        synchronized (lock) {
            // Claim the consumer, so a later page is handed over through offerPage instead of starting
            // a second one. See consumerStarted.
            consumerStarted = true;
        }
        consumer.accept(batchPagingIterator, lastThrowable);
    }

    @Override
    public void kill(Throwable t) {
        boolean shouldTriggerConsumer = false;
        synchronized (lock) {
            lastThrowable = t;
            if (consumerStarted == false) {
                // no active consumer - can "activate" it with a failure
                consumerStarted = true;
                shouldTriggerConsumer = true;
            }
        }
        batchPagingIterator.kill(t); // this causes a already active consumer to fail
        failPageRequest(t);
        if (shouldTriggerConsumer) {
            consumer.accept(null, t);
        }
    }

    @Override
    public String toString() {
        return "CumulativePageBucketReceiver{" +
               "nodeName='" + nodeName + '\'' +
               ", phaseId=" + phaseId +
               ", numBuckets=" + numBuckets +
               ", consumer=" + consumer +
               '}';
    }
}
