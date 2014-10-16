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
import io.crate.PartitionName;
import io.crate.analyze.AbstractDataAnalysis;
import io.crate.metadata.Routing;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.QueryAndFetchNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class PlanNodeBuilder {

    static MergeNode distributedMerge(QueryAndFetchNode queryAndFetchNode,
                                      ImmutableList<Projection> projections) {
        MergeNode node = new MergeNode("distributed merge", queryAndFetchNode.executionNodes().size());
        node.projections(projections);

        assert queryAndFetchNode.downStreamNodes()!=null && queryAndFetchNode.downStreamNodes().size()>0;
        node.executionNodes(ImmutableSet.copyOf(queryAndFetchNode.downStreamNodes()));
        connectTypes(queryAndFetchNode, node);
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
    static void setOutputTypes(QueryAndFetchNode node) {
        if (node.projections().isEmpty() && node.collectorProjections().isEmpty()) {
            node.outputTypes(Planner.extractDataTypes(node.toCollect()));
        } else if (node.projections().isEmpty()) {
            node.outputTypes(Planner.extractDataTypes(node.collectorProjections(), Planner.extractDataTypes(node.toCollect())));
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

    /**
     * create a new QueryAndFetchNode from the given analysis and other information.
     *
     * @param analysis the query analysis
     * @param toCollect the symbols to collect
     * @param collectorProjections the projections that process the collected results
     * @param projections projections executed during merge
     * @param partitionIdent if not null, this query is routed to this partition
     * @return a QueryAndFetchNode, configured and ready to be added to a plan
     */
    static QueryAndFetchNode queryAndFetch(AbstractDataAnalysis analysis,
                                           List<Symbol> toCollect,
                                           ImmutableList<Projection> collectorProjections,
                                           @Nullable List<Projection> projections,
                                           @Nullable String partitionIdent) {
        Routing routing = analysis.table().getRouting(analysis.whereClause());
        if (partitionIdent != null && routing.hasLocations()) {
            routing = filterRouting(routing, PartitionName.fromPartitionIdent(
                            analysis.table().ident().name(), partitionIdent).stringValue());
        }
        QueryAndFetchNode node = new QueryAndFetchNode("collect",
                routing,
                toCollect,
                ImmutableList.<Symbol>of(),
                null,
                null,
                null,
                null,
                null,
                collectorProjections,
                projections,
                analysis.whereClause(),
                analysis.rowGranularity(),
                analysis.table().isPartitioned());
        node.configure();
        return node;
    }

    private static Routing filterRouting(Routing routing, String includeTableName) {
        assert routing.hasLocations();
        assert includeTableName != null;
        Map<String, Map<String, Set<Integer>>> newLocations = new HashMap<>();

        for (Map.Entry<String, Map<String, Set<Integer>>> entry : routing.locations().entrySet()) {
            Map<String, Set<Integer>> tableMap = new HashMap<>();
            for (Map.Entry<String, Set<Integer>> tableEntry : entry.getValue().entrySet()) {
                if (includeTableName.equals(tableEntry.getKey())) {
                    tableMap.put(tableEntry.getKey(), tableEntry.getValue());
                }
            }
            if (tableMap.size()>0){
                newLocations.put(entry.getKey(), tableMap);
            }

        }
        return new Routing(newLocations);
    }

}
