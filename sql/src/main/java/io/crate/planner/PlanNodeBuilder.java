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
import io.crate.analyze.SelectAnalysis;
import io.crate.analyze.where.WhereClause;
import io.crate.metadata.Routing;
import io.crate.metadata.relation.AnalyzedQuerySpecification;
import io.crate.metadata.relation.TableRelation;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class PlanNodeBuilder {

    static CollectNode distributingCollect(int relationIdent,
                                           AbstractDataAnalysis analysis,
                                           List<Symbol> toCollect,
                                           List<String> downstreamNodes,
                                           ImmutableList<Projection> projections) {
        CollectNode node;
        if (analysis instanceof SelectAnalysis) {
            AnalyzedQuerySpecification querySpec = ((SelectAnalysis) analysis).querySpecification();
            TableInfo tableInfo = ((TableRelation) querySpec.sourceRelation()).tableInfo();
            Routing routing = tableInfo.getRouting(querySpec.whereClause());
            node = new CollectNode("distributing collect", routing);
            node.whereClause(querySpec.whereClause());
            node.isPartitioned(tableInfo.isPartitioned());
        } else {
            node = new CollectNode("distributing collect", analysis.table().getRouting(analysis.whereClause()));
            node.whereClause(analysis.whereClause());
            node.isPartitioned(analysis.table().isPartitioned());
        }
        node.maxRowGranularity(analysis.rowGranularity());
        node.downStreamNodes(downstreamNodes);
        node.toCollect(toCollect);
        node.projections(projections);
        node.relationIdent(relationIdent);

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

    static CollectNode collect(int relationIdent,
                               AbstractDataAnalysis analysis,
                               List<Symbol> toCollect,
                               ImmutableList<Projection> projections,
                               @Nullable String partitionIdent) {
        WhereClause whereClause;
        TableInfo tableInfo;
        if (analysis instanceof SelectAnalysis) {
            AnalyzedQuerySpecification querySpecification = ((SelectAnalysis) analysis).querySpecification();
            whereClause = querySpecification.whereClause();
            tableInfo = ((TableRelation) querySpecification.sourceRelation()).tableInfo();
        } else {
            tableInfo = analysis.table();
            whereClause = analysis.whereClause();
        }
        Routing routing = tableInfo.getRouting(whereClause);
        if (partitionIdent != null && routing.hasLocations()) {
            routing = filterRouting(routing, PartitionName.fromPartitionIdent(
                            tableInfo.ident().name(), partitionIdent).stringValue());
        }
        CollectNode node = new CollectNode("collect", routing);
        node.whereClause(whereClause);
        node.toCollect(toCollect);
        node.maxRowGranularity(analysis.rowGranularity());
        node.projections(projections);
        node.isPartitioned(tableInfo.isPartitioned());
        node.relationIdent(relationIdent);
        setOutputTypes(node);
        return node;
    }

    private static Routing filterRouting(Routing routing, String includeTableName) {
        assert routing.hasLocations();
        assert includeTableName != null;
        Map<String, Map<String, Set<Integer>>> newLocations = new HashMap<>();

        for (Map.Entry<String, Map<String, Set<Integer>>> entry : routing.locations().entrySet()) {
            Map<String, Set<Integer>> tableMap = new HashMap<>();
            newLocations.put(entry.getKey(), tableMap);
            for (Map.Entry<String, Set<Integer>> tableEntry : entry.getValue().entrySet()) {
                if (includeTableName.equals(tableEntry.getKey())) {
                    tableMap.put(tableEntry.getKey(), tableEntry.getValue());
                }
            }
        }
        return new Routing(newLocations);
    }

    static CollectNode collect(int relationIdent,
                               AbstractDataAnalysis analysis,
                               List<Symbol> toCollect,
                               ImmutableList<Projection> projections) {
        return collect(relationIdent, analysis, toCollect, projections, null);
    }
}
