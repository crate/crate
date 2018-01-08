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

package io.crate.jobs;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.operation.PageResultListener;

import java.util.Collection;

/**
 * A component which receives buckets from one or more upstreams and merges them together.
 *
 * <pre>
 *
 *     +----+    +----+
 *     | n1 |    | n2 |
 *     +----+    +----+
 *        \        /
 *         Downstream ({@link io.crate.jobs.PageBucketReceiver} (usually created from {@link io.crate.planner.node.dql.MergePhase}))
 *
 *  For example:
 *
 *   PageBucketReceiver has 2 upstream, so it expects 2 buckets.
 *
 *   n1: sends bucket with bucketIdx=0
 *   n2: sends bucket with bucketIdx=1
 *
 *  (bucketIdx definition is up to the upstreams, but it needs to be deterministic,
 *  see {@link io.crate.operation.projectors.DistributingDownstreamFactory#getBucketIdx(Collection)}
 *
 *   Once PageBucketReceiver has received all parts of a "Page" (in this case 2 buckets),
 *   it has to call {@link PageResultListener#needMore(boolean)} to indicate if it's done or that more data is needed.
 * </pre>
 *
 */
public interface PageBucketReceiver {

    /**
     * Receives a bucket from an upstream which holds result data. This method should be
     * called multiple times for each bucketIdx if the corresponding
     * {@code pageResultListener} is called via {@code PageResultListener#needMore(true)}.
     * @param bucketIdx A bucket id which uniquely identifies all buckets of this page.
     * @param rows The bucket which holds result rows.
     * @param isLast Indicates whether this is the last bucket with this id.
     * @param pageResultListener The ResultListener which is informed if more data is needed (for an additional page).
     */
    void setBucket(int bucketIdx, Bucket rows, boolean isLast, PageResultListener pageResultListener);

    void failure(int bucketIdx, Throwable throwable);

    void killed(int bucketIdx, Throwable throwable);

    Streamer<?>[] streamers();
}
