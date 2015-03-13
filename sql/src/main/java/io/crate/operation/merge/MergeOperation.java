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

package io.crate.operation.merge;

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.operation.DownstreamOperation;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.PageDownstream;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * merge rows - that's it
 */
public class MergeOperation implements DownstreamOperation {

    private final int numUpstreams;
    private final FlatProjectorChain projectorChain;
    private final BucketMerger bucketMerger;

    public MergeOperation(ClusterService clusterService,
                          Settings settings,
                          TransportActionProvider transportActionProvider,
                          ImplementationSymbolVisitor symbolVisitor,
                          ThreadPool threadPool,
                          MergeNode mergeNode,
                          RamAccountingContext ramAccountingContext) {
        // TODO: used by local and reducer, todo check what resultprovider is
        this.projectorChain = new FlatProjectorChain(mergeNode.projections(),
                new ProjectionToProjectorVisitor(
                        clusterService,
                        settings,
                        transportActionProvider,
                        symbolVisitor),
                ramAccountingContext
        );
        this.bucketMerger = new NonSortingBucketMerger(threadPool);
        this.bucketMerger.downstream(projectorChain.firstProjector());
        this.numUpstreams = mergeNode.numUpstreams();
        this.projectorChain.startProjections();
    }

    @Override
    public PageDownstream pageDownstream() {
        return bucketMerger;
    }

    @Override
    public int numUpstreams() {
        return numUpstreams;
    }

    @Override
    public void finished() {
        bucketMerger.finish();
    }

    public ListenableFuture<Bucket> result() {
        return projectorChain.resultProvider().result();
    }

}
