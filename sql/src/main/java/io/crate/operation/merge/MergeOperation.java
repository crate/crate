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
import io.crate.core.collections.Row;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.operation.DownstreamOperation;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.projectors.*;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * merge rows - that's it
 */
public class MergeOperation implements DownstreamOperation {

    private final int numUpstreams;
    private final ListenableFuture<Bucket> result;
    private RowDownstreamHandle downstream;

    private AtomicBoolean wantMore = new AtomicBoolean(true);
    private final Object lock = new Object();


    public MergeOperation(ClusterService clusterService,
                          Settings settings,
                          TransportActionProvider transportActionProvider,
                          ImplementationSymbolVisitor symbolVisitor,
                          MergeNode mergeNode,
                          RamAccountingContext ramAccountingContext) {
        // TODO: used by local and reducer, todo check what resultprovider is
        ProjectionToProjectorVisitor projectionToProjectorVisitor = new ProjectionToProjectorVisitor(
                clusterService,
                settings,
                transportActionProvider,
                symbolVisitor
        );
        // TODO: switch to use a streaming downstream here, which requires to refactor this operation
        FlatProjectorChain projectorChain = FlatProjectorChain.withResultProvider(
                projectionToProjectorVisitor,
                ramAccountingContext,
                mergeNode.projections()
        );
        this.result = projectorChain.resultProvider().result();
        this.downstream = projectorChain.firstProjector().registerUpstream(this);
        this.numUpstreams = mergeNode.numUpstreams();
        projectorChain.startProjections();
    }

    public boolean addRows(Bucket rows) throws Exception {
        for (Row row : rows) {
            boolean more = wantMore.get();
            if (more) {
                synchronized (lock) {
                    if (wantMore.get() && !downstream.setNextRow(row)) {
                        wantMore.set(false);
                        return false;
                    }
                }
            } else {
                return false;
            }
        }
        return wantMore.get();
    }

    @Override
    public int numUpstreams() {
        return numUpstreams;
    }

    @Override
    public void finished() {
        downstream.finish();
    }

    public ListenableFuture<Bucket> result() {
        return result;
    }

}
