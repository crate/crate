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
import io.crate.breaker.RamAccountingContext;
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
 * <li> construct it,
 * <li> call {@linkplain #startProjections()},
 * <li> get the first projector using {@linkplain #firstProjector()}
 * <li> feed data to it,
 * <li> and get the result of the {@linkplain #lastProjector()} with {@linkplain ResultProvider#result()}.
 */
public class FlatProjectorChain {

    private final List<Projector> projectors;

    public FlatProjectorChain(List<Projector> projectors) {
        this.projectors = projectors;
    }

    public FlatProjectorChain(List<Projection> projections,
                              ProjectionToProjectorVisitor projectorVisitor,
                              RamAccountingContext ramAccountingContext) {

        if (projections.size() > 0) {
            projectors = new ArrayList<>(projections.size() + 1);
            Projector previousProjector = null;
            for (Projection projection : projections) {
                Projector projector = projectorVisitor.process(projection, ramAccountingContext);
                projectors.add(projector);
                if (previousProjector != null) {
                    previousProjector.downstream(projector);
                }
                previousProjector = projector;
            }
        } else {
            projectors = new ArrayList<>();
        }
    }

    public void startProjections() {
        for (Projector projector : Lists.reverse(projectors)) {
            projector.startProjection();
        }
    }

    public Projector firstProjector() {
        return projectors.get(0);
    }

    public Projector lastProjector() {
        return projectors.get(projectors.size() - 1);
    }

    /**
     * appends a projector to the chain
     *
     * @param projector to be appended
     */
    public void append(Projector projector) {
        lastProjector().downstream(projector);
        projectors.add(projector);
    }

    public ResultProvider resultProvider() {
        if (projectors.isEmpty() || !(lastProjector() instanceof ResultProvider)) {
            append(new CollectingProjector());
        }
        return (ResultProvider) lastProjector();
    }

}
