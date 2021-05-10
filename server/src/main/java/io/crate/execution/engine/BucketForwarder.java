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

package io.crate.execution.engine;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.jobs.transport.JobResponse;
import org.elasticsearch.action.ActionListener;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Forwards buckets to {@link PageBucketReceiver}s
 */
final class BucketForwarder {

    private final List<PageBucketReceiver> pageBucketReceivers;
    private final int bucketIdx;
    private final InitializationTracker initializationTracker;

    static BiConsumer<List<? extends Bucket>, Throwable> asConsumer(List<PageBucketReceiver> pageBucketReceivers,
                                                                    int bucketIdx,
                                                                    InitializationTracker initializationTracker) {
        BucketForwarder forwarder = new BucketForwarder(pageBucketReceivers, bucketIdx, initializationTracker);
        return (buckets, failure) -> {
            if (failure == null) {
                forwarder.setBuckets(buckets);
            } else {
                forwarder.failed(SQLExceptions.unwrap(failure));
            }
        };
    }

    static ActionListener<JobResponse> asActionListener(List<PageBucketReceiver> pageBucketReceivers,
                                                        int bucketIdx,
                                                        InitializationTracker initializationTracker) {
        BucketForwarder forwarder = new BucketForwarder(pageBucketReceivers, bucketIdx, initializationTracker);
        return new BucketForwardingActionListener(pageBucketReceivers.get(0).streamers(), forwarder);
    }

    private BucketForwarder(List<PageBucketReceiver> pageBucketReceivers,
                            int bucketIdx,
                            InitializationTracker initializationTracker) {
        assert !pageBucketReceivers.isEmpty() : "pageBucketReceivers must not be empty";
        this.pageBucketReceivers = pageBucketReceivers;
        this.bucketIdx = bucketIdx;
        this.initializationTracker = initializationTracker;
    }

    protected void setBuckets(List<? extends Bucket> buckets) {
        initializationTracker.jobInitialized();
        for (int i = 0; i < pageBucketReceivers.size(); i++) {
            PageBucketReceiver pageBucketReceiver = pageBucketReceivers.get(i);
            Bucket bucket = buckets.get(i);
            assert bucket != null : "buckets must contain a non-null bucket at idx=" + i;
            pageBucketReceiver.setBucket(bucketIdx, bucket, true, PagingUnsupportedResultListener.INSTANCE);
        }
    }

    protected void failed(@Nonnull Throwable t) {
        initializationTracker.jobInitializationFailed(t);
        for (PageBucketReceiver pageBucketReceiver : pageBucketReceivers) {
            pageBucketReceiver.kill(t);
        }
    }

    private static class BucketForwardingActionListener implements ActionListener<JobResponse> {
        private final Streamer<?>[] responseDeserializer;
        private final BucketForwarder forwarder;

        BucketForwardingActionListener(Streamer<?>[] responseDeserializer, BucketForwarder forwarder) {
            this.responseDeserializer = responseDeserializer;
            this.forwarder = forwarder;
        }

        @Override
        public void onResponse(JobResponse jobResponse) {
            forwarder.setBuckets(jobResponse.getDirectResponses(responseDeserializer));
        }

        @Override
        public void onFailure(Exception e) {
            forwarder.failed(e);
        }
    }
}
