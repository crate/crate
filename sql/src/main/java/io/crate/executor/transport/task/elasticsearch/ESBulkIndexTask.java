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
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.task.AbstractChainedTask;
import io.crate.planner.node.dml.ESIndexNode;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class ESBulkIndexTask extends AbstractChainedTask<RowCountResult> {

    private final BulkShardProcessor bulkShardProcessor;
    private final ESIndexNode node;
    private final List<ListenableFuture<RowCountResult>> bulkResultList;

    public ESBulkIndexTask(ClusterService clusterService,
                           Settings settings,
                           TransportShardBulkAction transportShardBulkAction,
                           TransportCreateIndexAction transportCreateIndexAction,
                           ESIndexNode node) {
        this.node = node;
        this.bulkShardProcessor = new BulkShardProcessor(
                clusterService,
                settings,
                transportShardBulkAction,
                transportCreateIndexAction,
                node.partitionedTable(),
                true,
                this.node.sourceMaps().size());

        if (!node.isBulkRequest()) {
            bulkResultList = this.resultList;

            Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
                @Override
                public void onSuccess(@Nullable BitSet bulkShardProcessorResult) {
                    if (bulkShardProcessorResult == null) {
                        result.set(TaskResult.ROW_COUNT_UNKNOWN);
                    } else {
                        result.set(new RowCountResult(bulkShardProcessorResult.cardinality()));
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    result.setException(t);
                }
            });
        } else {
            final int numResults = node.sourceMaps().size();
            bulkResultList = new ArrayList<>(numResults);
            for (int i = 0; i < numResults; i++) {
                bulkResultList.add(SettableFuture.<RowCountResult>create());
            }
            Futures.addCallback(bulkShardProcessor.result(), new FutureCallback<BitSet>() {
                @Override
                public void onSuccess(@Nullable BitSet result) {
                    if (result == null) {
                        setAllToFailed(null);
                        return;
                    }

                    for (int i = 0; i < numResults; i++) {
                        SettableFuture<RowCountResult> future = (SettableFuture<RowCountResult>) bulkResultList.get(i);
                        future.set(result.get(i) ? TaskResult.ONE_ROW : TaskResult.FAILURE);
                    }
                }

                private void setAllToFailed(@Nullable Throwable throwable) {
                    if (throwable == null) {
                        for (ListenableFuture<RowCountResult> future : bulkResultList) {
                            ((SettableFuture<RowCountResult>) future).set(TaskResult.FAILURE);
                        }
                    } else {
                        for (ListenableFuture<RowCountResult> future : bulkResultList) {
                            ((SettableFuture<RowCountResult>) future).set(RowCountResult.error(throwable));
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
    protected void doStart(List<TaskResult> upstreamResults) {
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
    public List<ListenableFuture<RowCountResult>> result() {
        return bulkResultList;
    }
}
