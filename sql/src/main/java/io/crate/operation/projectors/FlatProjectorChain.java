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

package io.crate.operation.projectors;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.RowUpstream;
import io.crate.planner.projection.Projection;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A chain of connected projectors rows will flow through.
 *
 * Will be constructed from a list of projections which will be transformed to
 * projectors which will be connected.
 *
 * Usage:
 * <ul>
 * <li> construct it,
 * <li> call {@linkplain #startProjections()},
 * <li> get the first projector using {@linkplain #firstProjector()}
 * <li> feed data to it,
 * <li> and get the result of the {@linkplain #resultProvider()} with {@linkplain ResultProvider#result()}.
 */
public class FlatProjectorChain {

    private Projector firstProjector;
    private final List<Projector> projectors;
    private final ResultProvider resultProvider;

    public FlatProjectorChain(List<Projection> projections,
                              ProjectionToProjectorVisitor projectorVisitor,
                              RamAccountingContext ramAccountingContext) {
        this(projections, projectorVisitor, ramAccountingContext,
                Optional.<ResultProvider>absent());
    }

    public FlatProjectorChain(List<Projection> projections,
                              ProjectionToProjectorVisitor projectorVisitor,
                              RamAccountingContext ramAccountingContext,
                              Optional<ResultProvider> resultProvider) {
        this(projections, projectorVisitor, ramAccountingContext,
                resultProvider, Optional.<UUID>absent(),
                Optional.<IntObjectOpenHashMap<String>>absent(),
                Optional.<IntObjectOpenHashMap<ShardId>>absent());
    }

    public FlatProjectorChain(List<Projection> projections,
                              ProjectionToProjectorVisitor projectorVisitor,
                              RamAccountingContext ramAccountingContext,
                              Optional<ResultProvider> resultProvider,
                              Optional<UUID> jobId,
                              Optional<IntObjectOpenHashMap<String>> jobSearchContextIdToNode,
                              Optional<IntObjectOpenHashMap<ShardId>> jobSearchContextIdToShard) {
        if (projections.size() == 0) {
            if (resultProvider.isPresent()) {
                this.resultProvider = resultProvider.get();
            } else {
                this.resultProvider = new CollectingProjector();
            }
            assert (this.resultProvider() instanceof Projector);
            firstProjector = this.resultProvider;
            projectors = ImmutableList.of(firstProjector);
        } else {
            projectors = new ArrayList<>();
            Projector previousProjector = null;
            for (Projection projection : projections) {
                Projector projector = projectorVisitor.process(projection, ramAccountingContext,
                        jobId, jobSearchContextIdToNode, jobSearchContextIdToShard);
                projectors.add(projector);
                if (previousProjector != null) {
                    previousProjector.downstream(projector);
                } else {
                    firstProjector = projector;
                }
                assert projector instanceof RowUpstream :
                        "Cannot use a projector that is no ProjectorUpstream as upstream";
                previousProjector = projector;
            }
            assert previousProjector != null;
            final boolean addedResultProvider;
            if (resultProvider.isPresent()) {
                this.resultProvider = resultProvider.get();
                addedResultProvider = true;
            } else {
                if (previousProjector instanceof ResultProvider) {
                    this.resultProvider = (ResultProvider) previousProjector;
                    addedResultProvider = false;
                } else {
                    this.resultProvider = new CollectingProjector();
                    addedResultProvider = true;
                }
            }
            if (addedResultProvider) {
                previousProjector.downstream(this.resultProvider);
                projectors.add(this.resultProvider);
            }
        }
    }

    public void startProjections() {
        for (Projector projector : Lists.reverse(projectors)) {
            projector.startProjection();
        }
    }

    public Projector firstProjector() {
        return firstProjector;
    }

    public ResultProvider resultProvider() {
        return resultProvider;
    }
}
