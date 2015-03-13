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

import com.google.common.base.Optional;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.DownstreamOperation;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.PageDownstream;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ResultProvider;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * merge rows - that's it
 */
@Singleton
public class MergeOperation implements DownstreamOperation {

    private final ThreadPool threadPool;
    private final ProjectionToProjectorVisitor projectionToProjectorVisitor;

    @Inject
    public MergeOperation(ClusterService clusterService,
                          Settings settings,
                          TransportActionProvider transportActionProvider,
                          ReferenceResolver referenceResolver,
                          Functions functions,
                          ThreadPool threadPool) {

        this.threadPool = threadPool;
        ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver,
                functions,
                RowGranularity.DOC
        );
        this.projectionToProjectorVisitor = new ProjectionToProjectorVisitor(
                clusterService,
                settings,
                transportActionProvider,
                implementationSymbolVisitor
        );
    }

    public PageDownstream getAndInitPageDownstream(MergeNode mergeNode,
                                                   ResultProvider resultProvider,
                                                   RamAccountingContext ramAccountingContext) {
        BucketMerger bucketMerger = new NonSortingBucketMerger(threadPool);

        FlatProjectorChain flatProjectorChain = new FlatProjectorChain(mergeNode.projections(),
                this.projectionToProjectorVisitor,
                ramAccountingContext,
                Optional.of(resultProvider)
        );
        bucketMerger.downstream(flatProjectorChain.firstProjector());
        flatProjectorChain.startProjections();
        return bucketMerger;
    }
}
