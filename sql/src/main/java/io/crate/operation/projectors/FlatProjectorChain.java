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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.ProjectorUpstream;
import io.crate.planner.projection.Projection;

import java.util.ArrayList;
import java.util.List;

/**
 * A chain of connected projectors rows will flow through.
 *
 * Will be constructed from a list of projections which will be transformed to
 * projectors which will be connected.
 *
 * Usage:
 * <ul>
 *  <li> construct it,
 *  <li> call {@linkplain #startProjections()},
 *  <li> get the first projector using {@linkplain #firstProjector()}
 *  <li> feed data to it,
 *  <li> and get the result of the {@linkplain #lastProjector()} with {@linkplain ResultProvider#result()}.
 */
public class FlatProjectorChain {

    private Projector firstProjector;
    private final List<Projector> projectors;
    private ResultProvider lastProjector;

    public FlatProjectorChain(List<Projection> projections,
                              ProjectionToProjectorVisitor projectorVisitor,
                              RamAccountingContext ramAccountingContext) {
        projectors = new ArrayList<>();
        if (projections.size() == 0) {
            firstProjector = new CollectingProjector();
            lastProjector = (ResultProvider)firstProjector;
            projectors.add(firstProjector);
        } else {
            ProjectorUpstream previousProjector = null;
            for (Projection projection : projections) {
                Projector projector = projectorVisitor.process(projection, ramAccountingContext);
                projectors.add(projector);
                if (previousProjector != null) {
                    previousProjector.downstream(projector);
                } else {
                    firstProjector = projector;
                }
                assert projector instanceof ProjectorUpstream :
                        "Cannot use a projector that is no ProjectorUpstream as upstream";
                previousProjector = (ProjectorUpstream)projector;
            }

            assert previousProjector != null;
            if (previousProjector instanceof ResultProvider) {
                lastProjector = (ResultProvider)previousProjector;
            } else {
                lastProjector = new CollectingProjector();
                previousProjector.downstream((Projector)lastProjector);
            }
            assert firstProjector != null;
            assert lastProjector != null;
        }
    }

    public void startProjections() {
        for (Projector projector : Lists.reverse(projectors)) {
            projector.startProjection();
        }
    }

    public ResultProvider lastProjector() {
        return lastProjector;
    }

    public Projector firstProjector() {
        return firstProjector;
    }

    public ListenableFuture<Object[][]> result() {
        return lastProjector().result();
    }
}
