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

package io.crate.executor.transport.executionphases;

import com.google.common.util.concurrent.FutureCallback;
import io.crate.action.job.JobResponse;
import io.crate.core.collections.Bucket;
import io.crate.jobs.PageDownstreamContext;
import org.elasticsearch.action.ActionListener;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

class SetBucketAction implements FutureCallback<List<Bucket>>, ActionListener<JobResponse> {
    private final List<PageDownstreamContext> pageDownstreamContexts;
    private final int bucketIdx;
    private final InitializationTracker initializationTracker;
    private final BucketResultListener bucketResultListener;

    SetBucketAction(List<PageDownstreamContext> pageDownstreamContexts, int bucketIdx, InitializationTracker initializationTracker) {
        this.pageDownstreamContexts = pageDownstreamContexts;
        this.bucketIdx = bucketIdx;
        this.initializationTracker = initializationTracker;
        bucketResultListener = new BucketResultListener(bucketIdx);
    }

    @Override
    public void onSuccess(@Nullable List<Bucket> result) {
        initializationTracker.jobInitialized(null);
        if (result == null) {
            onFailure(new NullPointerException("result is null"));
            return;
        }

        for (int i = 0; i < pageDownstreamContexts.size(); i++) {
            PageDownstreamContext pageDownstreamContext = pageDownstreamContexts.get(i);
            setBucket(pageDownstreamContext, result.get(i));
        }
    }

    @Override
    public void onResponse(JobResponse jobResponse) {
        initializationTracker.jobInitialized(null);
        for (int i = 0; i < pageDownstreamContexts.size(); i++) {
            PageDownstreamContext pageDownstreamContext = pageDownstreamContexts.get(i);
            jobResponse.streamers(pageDownstreamContext.streamer());
            setBucket(pageDownstreamContext, jobResponse.directResponse().get(i));
        }
    }

    @Override
    public void onFailure(@Nonnull Throwable t) {
        initializationTracker.jobInitialized(t);
        for (PageDownstreamContext pageDownstreamContext : pageDownstreamContexts) {
            pageDownstreamContext.failure(bucketIdx, t);
        }
    }

    private void setBucket(PageDownstreamContext pageDownstreamContext, Bucket bucket) {
        if (bucket == null) {
            pageDownstreamContext.failure(bucketIdx, new IllegalStateException("expected directResponse but didn't get one"));
            return;
        }
        pageDownstreamContext.setBucket(bucketIdx, bucket, true, bucketResultListener);
    }
}
