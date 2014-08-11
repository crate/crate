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
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.annotation.Nullable;
import java.io.IOException;
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
                transportShardBulkAction, transportCreateIndexAction, false, this.node.sourceMaps().size());
    }

    @Override
    protected void doStart(List<Object[][]> upstreamResults) {
        for(int i=0; i < this.node.sourceMaps().size(); i++){

            try {
                bulkShardProcessor.add(node.indices()[node.indices().length == 1 ? 0 : i],
                        XContentFactory.jsonBuilder().map(node.sourceMaps().get(i)).bytes(),
                        node.ids().get(i),
                        node.routingValues().get(i)
                );
            } catch (IOException e) {
                result.setException(e);
            }
        }
        bulkShardProcessor.close();
        Futures.addCallback(bulkShardProcessor.result(), new com.google.common.util.concurrent.FutureCallback<Long>() {

            @Override
            public void onSuccess(@Nullable Long res) {
                result.set(new Object[][]{new Object[]{res}});
            }

            @Override
            public void onFailure(Throwable t) {
                result.setException(t);
            }
        });
    }
}
