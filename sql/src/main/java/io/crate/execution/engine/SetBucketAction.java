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

package io.crate.execution.engine;

import io.crate.data.Bucket;
import io.crate.execution.jobs.PageBucketReceiver;

import javax.annotation.Nonnull;
import java.util.List;

abstract class SetBucketAction {

    private final List<PageBucketReceiver> pageBucketReceivers;
    private final int bucketIdx;
    private final InitializationTracker initializationTracker;
    private final BucketResultListener bucketResultListener;

    SetBucketAction(List<PageBucketReceiver> pageBucketReceivers, int bucketIdx, InitializationTracker initializationTracker) {
        assert !pageBucketReceivers.isEmpty() : "pageBucketReceivers must not be empty";
        this.pageBucketReceivers = pageBucketReceivers;
        this.bucketIdx = bucketIdx;
        this.initializationTracker = initializationTracker;
        bucketResultListener = new BucketResultListener();
    }

    protected void setBuckets(List<Bucket> result) {
        initializationTracker.jobInitialized();
        for (int i = 0; i < pageBucketReceivers.size(); i++) {
            PageBucketReceiver pageBucketReceiver = pageBucketReceivers.get(i);
            Bucket bucket = result.get(i);
            assert bucket != null : "expected directResponse but didn't get one idx=" + i;
            pageBucketReceiver.setBucket(bucketIdx, bucket, true, bucketResultListener);
        }
    }

    protected void failed(@Nonnull Throwable t) {
        initializationTracker.jobInitializationFailed(t);
        for (PageBucketReceiver pageBucketReceiver : pageBucketReceivers) {
            pageBucketReceiver.failure(bucketIdx, t);
        }
    }
}
