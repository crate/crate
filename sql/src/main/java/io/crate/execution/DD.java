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

package io.crate.execution;

import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
import io.crate.executor.transport.distributed.MultiBucketBuilder;
import io.crate.executor.transport.distributed.TransportDistributedResultAction;
import org.elasticsearch.action.ActionListener;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class DD implements Receiver<Row> {

    private final MultiBucketBuilder bucketBuilder;
    private final TransportDistributedResultAction resultAction;
    private final UUID jobId;
    private final int phaseId;
    private final byte inputId;
    private final int bucketIdx;
    private final Streamer<?>[] streamers;
    private final int pageSize = 2;

    public DD(MultiBucketBuilder bucketBuilder,
              TransportDistributedResultAction resultAction,
              UUID jobId,
              int phaseId,
              byte inputId,
              int bucketIdx,
              Streamer<?>[] streamers) {
        this.bucketBuilder = bucketBuilder;
        this.resultAction = resultAction;
        this.jobId = jobId;
        this.phaseId = phaseId;
        this.inputId = inputId;
        this.bucketIdx = bucketIdx;
        this.streamers = streamers;
    }

    @Override
    public Result<Row> onNext(Row item) {
        bucketBuilder.add(item);
        if (bucketBuilder.size() == pageSize) {
            Bucket[] buckets = new Bucket[1];
            bucketBuilder.build(buckets);

            ResultListener listener = new ResultListener();

            resultAction.pushResult(
                "n1",
                new DistributedResultRequest(jobId, phaseId, inputId, bucketIdx, streamers, buckets[0], false),
                listener
            );

            return new Result<Row>() {
                @Override
                public Type type() {
                    return Type.SUSPEND;
                }

                @Override
                public CompletableFuture<ResumeHandle> continuation() {
                    return listener.thenApply(r -> (ResumeHandle) () -> {});
                }
            };
        }
        return Result.getContinue();
    }

    @Override
    public void onFinish() {
    }

    @Override
    public void onError(Throwable t) {
    }

    private class ResultListener
        extends CompletableFuture<DistributedResultResponse> implements ActionListener<DistributedResultResponse> {

        @Override
        public void onResponse(DistributedResultResponse distributedResultResponse) {
            complete(distributedResultResponse);
        }

        @Override
        public void onFailure(Throwable e) {
            completeExceptionally(e);
        }
    }
}
