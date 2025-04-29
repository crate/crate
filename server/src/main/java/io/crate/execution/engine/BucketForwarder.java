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

import java.util.List;

import org.elasticsearch.action.ActionListener;

import io.crate.data.Bucket;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.jobs.transport.JobResponse;

/**
 * Forwards buckets to {@link PageBucketReceiver}s
 */
final class BucketForwarder implements ActionListener<JobResponse> {

    private final List<PageBucketReceiver> bucketReceivers;
    private final int bucketIdx;
    private final InitializationTracker initializationTracker;

    BucketForwarder(List<PageBucketReceiver> bucketReceivers, int bucketIdx, InitializationTracker initializationTracker) {
        assert !bucketReceivers.isEmpty() : "bucketReceivers must not be empty";
        this.bucketReceivers = bucketReceivers;
        this.bucketIdx = bucketIdx;
        this.initializationTracker = initializationTracker;
    }

    @Override
    public void onResponse(JobResponse jobResponse) {
        initializationTracker.jobInitialized();
        List<StreamBucket> directResponses = jobResponse.getDirectResponses(bucketReceivers.getFirst().streamers());
        for (int i = 0; i < bucketReceivers.size(); i++) {
            PageBucketReceiver pageBucketReceiver = bucketReceivers.get(i);
            Bucket bucket = directResponses.get(i);
            assert bucket != null : "buckets must contain a non-null bucket at idx=" + i;
            pageBucketReceiver.setBucket(bucketIdx, bucket, true, PagingUnsupportedResultListener.INSTANCE);
        }
    }

    @Override
    public void onFailure(Exception e) {
        initializationTracker.jobInitializationFailed(e);
        for (PageBucketReceiver pageBucketReceiver : bucketReceivers) {
            pageBucketReceiver.kill(e);
        }
    }
}
