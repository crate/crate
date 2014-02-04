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

package io.crate.operator.projectors;

import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.planner.projection.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * TODO: implement
 */
public class ProjectionToProjectorVisitor extends ProjectionVisitor<ProjectionToProjectorVisitor.Context, Projector> {

    public static class Context {
        List<Projector> projectors = new ArrayList<>();

        /**
         * add the projector to the list of projectors
         * and connect this projector to the last gathered one by setting it as downStream.
         * @param projector
         */
        public void add(Projector projector) {
            if (!projectors.isEmpty()) {
                projectors.get(projectors.size()-1).setUpStream(projector);
            }
            projectors.add(projector);
        }

        public List<Projector> projectors() {
            return projectors;
        }
    }

    private final ImplementationSymbolVisitor symbolVisitor;

    public List<Projector> process(Collection<Projection> projections) {
        Context ctx = new Context();
        process(projections, ctx);
        return ctx.projectors();
    }

    public ProjectionToProjectorVisitor(ImplementationSymbolVisitor symbolVisitor) {
        this.symbolVisitor = symbolVisitor;
    }

    @Override
    public Projector visitColumnProjection(ColumnProjection projection, Context context) {
        return super.visitColumnProjection(projection, context);
    }

    @Override
    public Projector visitTopNProjection(TopNProjection projection, Context context) {
        return super.visitTopNProjection(projection, context);
    }

    @Override
    public Projector visitGroupProjection(GroupProjection projection, Context context) {
        return super.visitGroupProjection(projection, context);
    }
}
