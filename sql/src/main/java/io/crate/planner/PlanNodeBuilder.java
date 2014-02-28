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

package io.crate.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.Analysis;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;

import java.util.List;

class PlanNodeBuilder {

    static CollectNode distributingCollect(Analysis analysis,
                                           List<Symbol> toCollect,
                                           List<String> downstreamNodes,
                                           ImmutableList<Projection> projections) {
        CollectNode node = new CollectNode("distributing collect", analysis.table().getRouting(analysis.whereClause()));
        node.whereClause(analysis.whereClause());
        node.maxRowGranularity(analysis.rowGranularity());
        node.downStreamNodes(downstreamNodes);
        node.toCollect(toCollect);
        node.projections(projections);

        setOutputTypes(node);
        return node;
    }

    static MergeNode distributedMerge(CollectNode collectNode,
                                      ImmutableList<Projection> projections) {
        MergeNode node = new MergeNode("distributed merge", collectNode.executionNodes().size());
        node.projections(projections);

        assert collectNode.downStreamNodes()!=null && collectNode.downStreamNodes().size()>0;
        node.executionNodes(ImmutableSet.copyOf(collectNode.downStreamNodes()));
        connectTypes(collectNode, node);
        return node;
    }

    static MergeNode localMerge(List<Projection> projections,
                                DQLPlanNode previousNode) {
        MergeNode node = new MergeNode("localMerge", previousNode.executionNodes().size());
        node.projections(projections);
        connectTypes(previousNode, node);
        return node;
    }

    /**
     * calculates the outputTypes using the projections and input types.
     * must be called after projections have been set.
     */
    static void setOutputTypes(CollectNode node) {
        if (node.projections().isEmpty()) {
            node.outputTypes(Planner.extractDataTypes(node.toCollect()));
        } else {
            node.outputTypes(Planner.extractDataTypes(node.projections(), Planner.extractDataTypes(node.toCollect())));
        }
    }

    /**
     * sets the inputTypes from the previousNode's outputTypes
     * and calculates the outputTypes using the projections and input types.
     * <p/>
     * must be called after projections have been set
     */
    static void connectTypes(DQLPlanNode previousNode, DQLPlanNode nextNode) {
        nextNode.inputTypes(previousNode.outputTypes());
        nextNode.outputTypes(Planner.extractDataTypes(nextNode.projections(), nextNode.inputTypes()));
    }

    static CollectNode collect(Analysis analysis,
                               List<Symbol> toCollect,
                               ImmutableList<Projection> projections) {
        CollectNode node = new CollectNode("collect", analysis.table().getRouting(analysis.whereClause()));
        node.whereClause(analysis.whereClause());
        node.toCollect(toCollect);
        node.maxRowGranularity(analysis.rowGranularity());
        node.projections(projections);

        setOutputTypes(node);
        return node;
    }
}
