/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.breaker.RamAccountingContext;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A {@link DownstreamRXTask} which receives paged buckets from upstreams
 * and forwards the merged bucket results to the consumers for further processing.
 */
public class DistResultRXTask extends AbstractTask implements DownstreamRXTask {

    private final String name;
    private final RamAccountingContext ramAccountingContext;
    private final int numBuckets;
    private final PageBucketReceiver pageBucketReceiver;

    public DistResultRXTask(Logger logger,
                            int id,
                            String name,
                            PageBucketReceiver pageBucketReceiver,
                            RamAccountingContext ramAccountingContext,
                            int numBuckets) {
        super(id, logger);
        this.name = name;
        this.ramAccountingContext = ramAccountingContext;
        this.numBuckets = numBuckets;
        this.pageBucketReceiver = pageBucketReceiver;

        this.pageBucketReceiver.completionFuture().whenComplete((result, ex) -> {
            if (ex instanceof IllegalStateException) {
                kill(ex);
            } else {
                releaseListenersAndCloseContext(ex);
            }
        });
    }

    private void releaseListenersAndCloseContext(@Nullable Throwable throwable) {
        pageBucketReceiver.releasePageResultListeners();
        close(throwable);
    }

    @Override
    protected void innerClose(@Nullable Throwable throwable) {
        setBytesUsed(ramAccountingContext.totalBytes());
        ramAccountingContext.close();
    }

    @Override
    protected void innerKill(@Nonnull Throwable t) {
        pageBucketReceiver.kill(t);
    }

    @Override
    protected void innerStart() {
        // E.g. If the upstreamPhase is a collectPhase for a partitioned table without any partitions
        // there won't be any executionNodes for that collectPhase
        // -> no upstreams -> just finish
        if (numBuckets == 0) {
            pageBucketReceiver.consumeRows();
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "DistResultRXTask{" +
               "id=" + id() +
               ", numBuckets=" + numBuckets +
               ", closed=" + isClosed() +
               '}';
    }

    /**
     * The default behavior is to receive all upstream buckets,
     * regardless of the input id. For a {@link DownstreamRXTask}
     * which uses the inputId, see {@link JoinTask}.
     */
    @Nullable
    @Override
    public PageBucketReceiver getBucketReceiver(byte inputId) {
        return pageBucketReceiver;
    }
}
