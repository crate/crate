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

package io.crate.operator.operations.collect;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.planner.node.CollectNode;
import org.cratedb.Constants;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class DistributingCollectOperation extends LocalDataCollectOperation {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final TransportService transportService;

    @Inject
    public DistributingCollectOperation(Functions functions,
                                        ReferenceResolver referenceResolver,
                                        IndicesService indicesService,
                                        ThreadPool threadPool,
                                        ClusterService clusterService,
                                        TransportService transportService) {
        super(clusterService, functions, referenceResolver, indicesService, threadPool);
        this.transportService = transportService;
    }


    @Override
    protected ListenableFuture<Object[][]> handleNodeCollect(CollectNode collectNode) {
        throw new UnsupportedOperationException("no need for distributing collect on node level");
    }

    @Override
    protected ListenableFuture<Object[][]> handleShardCollect(CollectNode collectNode) {
        SettableFuture<Object[][]> pseudoResult = SettableFuture.create();


        pseudoResult.set(Constants.EMPTY_RESULT);
        return pseudoResult;
    }
}
