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

import com.google.common.util.concurrent.Futures;
import io.crate.planner.node.dml.ESIndexNode;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.BulkShardProcessor;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class ESBulkIndexTask extends AbstractESIndexTask {

    private final BulkShardProcessor bulkShardProcessor;

    public ESBulkIndexTask(ClusterService clusterService,
                           Settings settings,
                           TransportShardBulkAction transportShardBulkAction,
                           TransportCreateIndexAction transportCreateIndexAction,
                           ESIndexNode node) {
        super(node);
        this.bulkShardProcessor = new BulkShardProcessor(clusterService, settings,
                transportShardBulkAction, transportCreateIndexAction, node.partitionedTable(), this.node.sourceMaps().size());
    }

    @Override
    protected void doStart(List<Object[][]> upstreamResults) {
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
        Futures.addCallback(bulkShardProcessor.result(), new com.google.common.util.concurrent.FutureCallback<Long>() {

            @Override
            public void onSuccess(@Nullable Long res) {
                result.set(new Object[][]{new Object[]{res}});
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                result.setException(t);
            }
        });
    }
}
