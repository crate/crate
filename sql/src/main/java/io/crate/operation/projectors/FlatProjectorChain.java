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

import io.crate.planner.projection.Projection;

import java.util.List;

public class FlatProjectorChain {

    private final ProjectionToProjectorVisitor projectorVisitor;
    private Projector firstProjector;
    private Projector lastProjector;

    public FlatProjectorChain(List<Projection> projections, ProjectionToProjectorVisitor projectorVisitor) {

        this.projectorVisitor = projectorVisitor;
        if (projections.size() == 0) {
            firstProjector = lastProjector = new CollectingProjector();
        } else {
            Projector previousProjector = null;
            for (Projection projection : projections) {
                Projector projector = projectorVisitor.process(projection);
                if (previousProjector != null) {
                    previousProjector.setDownStream(projector);
                } else {
                    firstProjector = projector;
                }
                previousProjector = projector;
            }
            lastProjector = previousProjector;
            assert firstProjector != null;
            assert lastProjector != null;
        }
    }

    public void startProjections() {
        Projector projector = firstProjector;
        while (projector != null) {
            projector.startProjection();
            projector = projector.getDownstream();
        }
    }

    public void finishProjections() {
        Projector projector = firstProjector;
        while (projector != null) {
            projector.finishProjection();
            projector = projector.getDownstream();
        }
    }

    public Projector lastProjector() {
        return lastProjector;
    }

    public Projector firstProjector() {
        return firstProjector;
    }

    public Object[][] result() {
        return lastProjector().getRows();
    }

}
