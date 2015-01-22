/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.consumer;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.UpdateAnalyzedStatement;
import io.crate.analyze.VersionRewriter;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.Planner;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dml.UpdateByIdNode;
import io.crate.planner.node.dml.Update;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.UpdateProjection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.LongType;
import org.elasticsearch.cluster.routing.operation.plain.Preference;

import java.util.*;

public class UpdateConsumer implements Consumer {
    private final Visitor visitor;
    protected final AggregationProjection localMergeProjection;

    public UpdateConsumer(AnalysisMetaData analysisMetaData) {
        visitor = new Visitor(analysisMetaData);
        localMergeProjection = new AggregationProjection(
                Arrays.asList(new Aggregation(
                                analysisMetaData.functions().getSafe(
                                        new FunctionIdent(SumAggregation.NAME, Arrays.<DataType>asList(LongType.INSTANCE))
                                ).info(),
                                Arrays.<Symbol>asList(new InputColumn(0, DataTypes.LONG)),
                                Aggregation.Step.ITER,
                                Aggregation.Step.FINAL
                        )
                )
        );
    }

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        PlannedAnalyzedRelation plannedAnalyzedRelation = visitor.process(rootRelation, null);
        if (plannedAnalyzedRelation == null) {
            return false;
        }
        context.rootRelation(plannedAnalyzedRelation);
        return true;
    }

    class Visitor extends AnalyzedRelationVisitor<Void, PlannedAnalyzedRelation> {

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData) {
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public PlannedAnalyzedRelation visitUpdateAnalyzedStatement(UpdateAnalyzedStatement statement, Void context) {
            assert statement.sourceRelation() instanceof TableRelation : "sourceRelation of update statement must be a TableRelation";
            TableRelation tableRelation = (TableRelation) statement.sourceRelation();
            TableInfo tableInfo = tableRelation.tableInfo();

            if (tableInfo.schemaInfo().systemSchema() || tableInfo.rowGranularity() != RowGranularity.DOC) {
                return null;
            }

            List<List<DQLPlanNode>> childNodes = new ArrayList<>(statement.nestedStatements().size());
            for (UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis : statement.nestedStatements()) {
                WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
                WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(nestedAnalysis.whereClause());
                WhereClause whereClause = whereClauseContext.whereClause();
                if (whereClause.noMatch()){
                    continue;
                }
                if (whereClauseContext.ids().size() >= 1
                        && whereClauseContext.routingValues().size() == whereClauseContext.ids().size()) {
                    childNodes.add(updateById(nestedAnalysis, tableInfo, whereClause, whereClauseContext));
                } else {
                    childNodes.add(updateByQuery(nestedAnalysis, tableInfo, whereClause, whereClauseContext));
                }
            }
            if (childNodes.size()>0){
                return new Update(childNodes);
            } else {
                return new NoopPlannedAnalyzedRelation(statement);
            }
        }

        private List<DQLPlanNode> updateByQuery(UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis,
                                               TableInfo tableInfo,
                                               WhereClause whereClause,
                                               WhereClauseContext whereClauseContext) {

            if(whereClauseContext.whereClause().version().isPresent()){
                VersionRewriter versionRewriter = new VersionRewriter();
                Symbol whereClauseQuery = versionRewriter.rewrite(whereClause.query());
                whereClause = new WhereClause(whereClauseQuery);
            }

            if (!whereClause.noMatch() || !(tableInfo.isPartitioned() && whereClause.partitions().isEmpty())) {
                // for updates, we always need to collect the `_uid`
                Reference uidReference = new Reference(
                        new ReferenceInfo(
                                new ReferenceIdent(tableInfo.ident(), "_uid"),
                                RowGranularity.DOC, DataTypes.STRING));

                UpdateProjection updateProjection = new UpdateProjection(
                        new InputColumn(0, DataTypes.STRING),
                        convertAssignments(nestedAnalysis.assignments()),
                        whereClauseContext.whereClause().version().orNull());

                CollectNode collectNode = PlanNodeBuilder.collect(
                        tableInfo,
                        whereClause,
                        ImmutableList.<Symbol>of(uidReference),
                        ImmutableList.<Projection>of(updateProjection),
                        null,
                        Preference.PRIMARY.type()
                );
                MergeNode mergeNode = PlanNodeBuilder.localMerge(
                        ImmutableList.<Projection>of(localMergeProjection), collectNode);
                return ImmutableList.<DQLPlanNode>of(collectNode, mergeNode);
            } else {
                return ImmutableList.of();
            }
        }

        private List<DQLPlanNode> updateById(UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis,
                                             TableInfo tableInfo,
                                             WhereClause whereClause,
                                             WhereClauseContext whereClauseContext) {
            String[] indices = Planner.indices(tableInfo, whereClause);
            assert indices.length == 1;
            assert whereClauseContext.ids().size() == whereClauseContext.routingValues().size();
            List<DQLPlanNode> nodes = new ArrayList<>(whereClauseContext.ids().size());
            for (int i = 0; i < whereClauseContext.ids().size(); i++) {
                nodes.add(new UpdateByIdNode(
                                indices[0],
                                whereClauseContext.ids().get(i),
                                whereClauseContext.routingValues().get(i),
                                convertAssignments(nestedAnalysis.assignments()),
                                whereClause.version(),
                                null));
            }
            return nodes;
        }

        private Map<String, Symbol> convertAssignments(Map<Reference, Symbol> assignments) {
            Map<String, Symbol> convertedAssignments = new HashMap<>(assignments.size());
            for(Map.Entry<Reference, Symbol> entry : assignments.entrySet()) {
                convertedAssignments.put(entry.getKey().info().ident().columnIdent().fqn(), entry.getValue());
            }
            return convertedAssignments;
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            return null;
        }
    }
}
