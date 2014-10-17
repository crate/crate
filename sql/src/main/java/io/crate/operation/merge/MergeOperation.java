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
import io.crate.executor.transport.TransportActionProvider;
import io.crate.operation.DownstreamOperation;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.planner.projection.Projection;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Holds a number of chained projections
 * and puts all rows received through {@linkplain #addRows(Object[][])}
 * through them.
 * The final result of type <code>Object[][]</code> will be returned in a
 * {@linkplain com.google.common.util.concurrent.ListenableFuture}
 * when calling {@linkplain #result()}.
 */
public class MergeOperation implements DownstreamOperation {

    private final int numUpstreams;
    private final FlatProjectorChain projectorChain;
    private Projector downstream;

    private AtomicBoolean wantMore = new AtomicBoolean(true);

    public MergeOperation(ClusterService clusterService,
                          Settings settings,
                          TransportActionProvider transportActionProvider,
                          ImplementationSymbolVisitor symbolVisitor,
                          int numUpstreams,
                          List<Projection> projections) {
        projectorChain = new FlatProjectorChain(projections,
                new ProjectionToProjectorVisitor(
                        clusterService,
                        settings,
                        transportActionProvider,
                        symbolVisitor)
        );
        downstream(projectorChain.firstProjector());
        this.numUpstreams = numUpstreams;
        projectorChain.startProjections();
    }

    /**
     * process the given array of <code>Object[]</code> rows with the
     * projections hold by this class.
     */
    public boolean addRows(Object[][] rows) throws Exception {
        for (int i = 0, length = rows.length; i < length && wantMore.get(); i++) {
            // assume that all projectors .setNextRow(...) methods are threadsafe
            Object[] row = rows[i];
            if (!downstream.setNextRow(row)) {
                wantMore.set(false);
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

    public ListenableFuture<Object[][]> result() {
        return projectorChain.result();
    }

    @Override
    public void downstream(Projector downstream) {
        downstream.registerUpstream(this);
        this.downstream = downstream;
    }

    @Override
    public Projector downstream() {
        return downstream;
    }
}
