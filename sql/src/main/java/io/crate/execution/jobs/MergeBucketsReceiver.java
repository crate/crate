/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.jobs;

import io.crate.Streamer;
import io.crate.data.Bucket;
import org.apache.logging.log4j.Logger;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A {@link PageBucketReceiver} which receives paged buckets from upstreams
 * and forwards the merged bucket results to the consumers for further processing.
 */
public class MergeBucketsReceiver implements PageBucketReceiver {

    private final Object lock = new Object();
    private final Logger logger;
    private final String nodeName;
    private final int phaseId;
    private final boolean traceEnabled;
    private final Streamer<?>[] streamers;
    private final int numBuckets;
    private final Set<Integer> bucketIds;
    private final Set<Integer> exhaustedBucketIds;
    private final Map<Integer, PageResultListener> listenersByBucketIds;
    private final Map<Integer, Bucket> bucketsByIds;
    private final Consumer<Throwable> onIllegalStateKillOperation;
    private final Runnable onTriggerConsumer;
    private final Consumer<Map<Integer, Bucket>> onMergeBuckets;
    private final Throwable[] lastEncounteredThrowable;

    public MergeBucketsReceiver(Logger logger,
                                String nodeName,
                                int phaseId,
                                Streamer<?>[] streamers,
                                Set<Integer> bucketIds,
                                Set<Integer> exhaustedBucketIds,
                                Map<Integer, Bucket> bucketsByIds,
                                Map<Integer, PageResultListener> listenersByBucketIds,
                                Consumer<Throwable> onIllegalStateKillOperation,
                                Runnable onTriggerConsumer,
                                Consumer<Map<Integer, Bucket>> onMergeBuckets,
                                Throwable[] lastEncounteredThrowable,
                                int numBuckets) {
        this.logger = logger;
        traceEnabled = logger.isTraceEnabled();
        this.nodeName = nodeName;
        this.phaseId = phaseId;
        this.streamers = streamers;
        this.numBuckets = numBuckets;
        this.bucketIds = bucketIds;
        this.exhaustedBucketIds = exhaustedBucketIds;
        this.bucketsByIds = bucketsByIds;
        this.listenersByBucketIds = listenersByBucketIds;
        this.onIllegalStateKillOperation = onIllegalStateKillOperation;
        this.onTriggerConsumer = onTriggerConsumer;
        this.onMergeBuckets = onMergeBuckets;
        this.lastEncounteredThrowable = lastEncounteredThrowable;
    }

    @Override
    public void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener) {
        synchronized (bucketIds) {
            bucketIds.add(bucketIdx);
            if (lastEncounteredThrowable[0] == null) {
                listenersByBucketIds.put(bucketIdx, pageResultListener);
            } else {
                pageResultListener.needMore(false);
            }
        }
        boolean shouldTriggerConsumer = false;
        synchronized (lock) {
            traceLog("method=setBucket", bucketIdx);

            if (bucketsByIds.putIfAbsent(bucketIdx, rows) != null) {
                onIllegalStateKillOperation.accept(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Same bucket of a page set more than once. node=%s method=setBucket phaseId=%d bucket=%d",
                    nodeName, phaseId, bucketIdx)));
            }
            setExhaustedUpstreams();
            if (isLast) {
                exhaustedBucketIds.add(bucketIdx);
            }
            if (bucketsByIds.size() == numBuckets) {
                shouldTriggerConsumer = true;
            }
        }
        if (shouldTriggerConsumer) {
            onMergeBuckets.accept(bucketsByIds);
            onTriggerConsumer.run();
        } else if (isLast) {
            // release listener early here, otherwise other upstreams will be blocked
            // e.g. if 2 downstream contexts are used in the chain
            //      Phase -> DistributingDownstream -> Phase -> DistributingDownstream
            pageResultListener.needMore(false);
        }
    }


    private void traceLog(String msg, int bucketIdx) {
        if (traceEnabled) {
            logger.trace("{} phaseId={} bucket={}", msg, phaseId, bucketIdx);
        }
    }

    private void traceLog(String msg, int bucketIdx, Throwable t) {
        if (traceEnabled) {
            logger.trace("{} phaseId={} bucket={} throwable={}", msg, phaseId, bucketIdx, t);
        }
    }

    @Override
    public void failure(int bucketIdx, Throwable throwable) {
        traceLog("method=failure", bucketIdx, throwable);

        boolean shouldTriggerConsumer;
        synchronized (lock) {
            if (bucketsByIds.putIfAbsent(bucketIdx, Bucket.EMPTY) != null) {
                onIllegalStateKillOperation.accept(new IllegalStateException(String.format(Locale.ENGLISH,
                    "Same bucket of a page set more than once. node=%s method=failure phaseId=%d bucket=%d",
                    nodeName, phaseId, bucketIdx)));
                return;
            }
            shouldTriggerConsumer = setBucketFailure(bucketIdx, throwable);
        }
        if (shouldTriggerConsumer) {
            onTriggerConsumer.run();
        }
    }

    @Override
    public void killed(int bucketIdx, Throwable throwable) {
        traceLog("method=killed", bucketIdx, throwable);

        boolean shouldTriggerConsumer;
        synchronized (lock) {
            if (bucketsByIds.putIfAbsent(bucketIdx, Bucket.EMPTY) != null) {
                traceLog("method=killed future already set", bucketIdx);
                return;
            }
            shouldTriggerConsumer = setBucketFailure(bucketIdx, throwable);
        }
        if (shouldTriggerConsumer) {
            onTriggerConsumer.run();
        }
    }

    private boolean setBucketFailure(int bucketIdx, Throwable throwable) {
        // can't trigger failure on pageDownstream immediately as it would remove the context which the other
        // upstreams still require

        lastEncounteredThrowable[0] = throwable;
        exhaustedBucketIds.add(bucketIdx);
        return bucketsByIds.size() == numBuckets;
    }

    /**
     * need to set the futures of all upstreams that are exhausted as there won't come any more buckets from those upstreams
     */
    private void setExhaustedUpstreams() {
        exhaustedBucketIds.forEach(this::setToEmptyBucket);
    }

    private void setToEmptyBucket(int idx) {
        bucketsByIds.putIfAbsent(idx, Bucket.EMPTY);
    }

    @Override
    public Streamer<?>[] streamers() {
        return streamers;
    }
}
