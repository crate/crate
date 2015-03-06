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
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * merge rows - that's it
 */
public class MergeOperation implements DownstreamOperation {

    private final int numUpstreams;
    private final FlatProjectorChain projectorChain;
    private Projector downstream;

    private AtomicBoolean wantMore = new AtomicBoolean(true);
    private final Object lock = new Object();

    public MergeOperation(ClusterService clusterService,
                          Settings settings,
                          TransportActionProvider transportActionProvider,
                          ImplementationSymbolVisitor symbolVisitor,
                          MergeNode mergeNode,
                          RamAccountingContext ramAccountingContext) {
        // TODO: used by local and reducer, todo check what resultprovider is
        projectorChain = new FlatProjectorChain(mergeNode.projections(),
                new ProjectionToProjectorVisitor(
                        clusterService,
                        settings,
                        transportActionProvider,
                        symbolVisitor),
                ramAccountingContext
        );
        downstream(projectorChain.firstProjector());
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
        downstream.upstreamFinished();
    }

    public ListenableFuture<Bucket> result() {
        return projectorChain.resultProvider().result();
    }

    @Override
    public void downstream(Projector downstream) {
        downstream.registerUpstream(this);
        this.downstream = downstream;
    }
}
