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

package io.crate.operator.operations.merge;

import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.projectors.ProjectionToProjectorVisitor;
import io.crate.operator.projectors.Projector;
import io.crate.planner.node.dql.MergeNode;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * merge rows - that's it
 */
public class MergeOperation implements DownstreamOperation {

    private final List<Projector> projectors;
    private final Projector firstProjector;
    private final int numUpstreams;

    private AtomicBoolean wantMore = new AtomicBoolean(true);

    public MergeOperation(ImplementationSymbolVisitor symbolVisitor, MergeNode mergeNode) {
        ProjectionToProjectorVisitor projectorVisitor = new ProjectionToProjectorVisitor(symbolVisitor);
        this.projectors = projectorVisitor.process(mergeNode.projections());
        assert this.projectors.size() > 0;
        this.firstProjector = this.projectors.get(0);
        this.firstProjector.startProjection();
        this.numUpstreams = mergeNode.numUpstreams();
    }

    public boolean addRows(Object[][] rows) throws Exception{
        for (int i=0, length=rows.length; i< length && wantMore.get(); i++) {
            // assume that all projectors .setNextRow(...) methods are threadsafe
            if(!firstProjector.setNextRow(rows[i])) {
                wantMore.set(false);
            }
        }
        return wantMore.get();
    }

    @Override
    public int numUpstreams() {
        return numUpstreams;
    }

    public Object[][] result() throws Exception  {
        firstProjector.finishProjection();
        return projectors.get(projectors.size()-1).getRows();
    }
}
