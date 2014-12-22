/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.JobTask;
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.planner.node.dml.ESIndexNode;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.action.bulk.TransportShardBulkActionDelegate;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;

public class ESBulkIndexTask extends JobTask {

    private final BulkShardProcessor bulkShardProcessor;
    private final ESIndexNode node;
    private final ArrayList<ListenableFuture<TaskResult>> resultList;

    public ESBulkIndexTask(UUID jobId,
                           ClusterService clusterService,
                           Settings settings,
                           TransportShardBulkActionDelegate transportShardBulkActionDelegate,
                           TransportCreateIndexAction transportCreateIndexAction,
                           ESIndexNode node) {
        super(jobId);
        this.node = node;
        this.bulkShardProcessor = new BulkShardProcessor(
                clusterService,
                settings,
                transportShardBulkActionDelegate,
                transportCreateIndexAction,
                node.partitionedTable(),
                true,
                this.node.sourceMaps().size());

        if (!node.isBulkRequest()) {
            final SettableFuture<TaskResult> futureResult = SettableFuture.create();
            resultList = new ArrayList<>(1);
            resultList.add(futureResult);

            Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
                @Override
                public void onSuccess(@Nullable BitSet result) {
                    if (result == null) {
                        futureResult.set(TaskResult.ROW_COUNT_UNKNOWN);
                    } else {
                        futureResult.set(new RowCountResult(result.cardinality()));
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    futureResult.setException(t);
                }
            });
        } else {
            final int numResults = node.sourceMaps().size();
            resultList = new ArrayList<>(numResults);
            for (int i = 0; i < numResults; i++) {
                resultList.add(SettableFuture.<TaskResult>create());
            }
            Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
                @Override
                public void onSuccess(@Nullable BitSet result) {
                    if (result == null) {
                        setAllToFailed(null);
                        return;
                    }

                    for (int i = 0; i < numResults; i++) {
                        SettableFuture<TaskResult> future = (SettableFuture<TaskResult>) resultList.get(i);
                        future.set(result.get(i) ? TaskResult.ONE_ROW : TaskResult.FAILURE);
                    }
                }

                private void setAllToFailed(@Nullable Throwable throwable) {
                    if (throwable == null) {
                        for (ListenableFuture<TaskResult> future : resultList) {
                            ((SettableFuture<TaskResult>) future).set(TaskResult.FAILURE);
                        }
                    } else {
                        for (ListenableFuture<TaskResult> future : resultList) {
                            ((SettableFuture<TaskResult>) future).set(RowCountResult.error(throwable));
                        }
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    setAllToFailed(t);
                }
            });
        }
    }

    @Override
    public void start() {
        if (node.indices().length == 1) {
            String index = node.indices()[0];
            for(int i=0; i < this.node.sourceMaps().size(); i++){
                bulkShardProcessor.add(
                        index,
                        node.sourceMaps().get(i),
                        node.ids().get(i),
                        node.routingValues().get(i)
                );
            }
        } else {
            for(int i=0; i < this.node.sourceMaps().size(); i++){
                bulkShardProcessor.add(
                        node.indices()[i],
                        node.sourceMaps().get(i),
                        node.ids().get(i),
                        node.routingValues().get(i)
                );
            }
        }
        bulkShardProcessor.close();
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return resultList;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("BulkIndexTask can't have an upstream result");
    }
}
