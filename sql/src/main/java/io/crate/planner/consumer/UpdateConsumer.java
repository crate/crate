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
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.DocKeys;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.Planner;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dml.SymbolBasedUpsertByIdNode;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.UpdateProjection;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.LongType;
import org.elasticsearch.cluster.routing.operation.plain.Preference;
import org.elasticsearch.common.collect.Tuple;

import java.util.*;

public class UpdateConsumer implements Consumer {
    private final Visitor visitor;
    protected final AggregationProjection localMergeProjection;

    public UpdateConsumer(AnalysisMetaData analysisMetaData) {
        visitor = new Visitor();
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
        PlannedAnalyzedRelation plannedAnalyzedRelation = visitor.process(rootRelation, context);
        if (plannedAnalyzedRelation == null) {
            return false;
        }
        context.rootRelation(plannedAnalyzedRelation);
        return true;
    }

    class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        @Override
        public PlannedAnalyzedRelation visitUpdateAnalyzedStatement(UpdateAnalyzedStatement statement, ConsumerContext context) {
            assert statement.sourceRelation() instanceof TableRelation : "sourceRelation of update statement must be a TableRelation";
            TableRelation tableRelation = (TableRelation) statement.sourceRelation();
            TableInfo tableInfo = tableRelation.tableInfo();

            if (tableInfo.schemaInfo().systemSchema() || tableInfo.rowGranularity() != RowGranularity.DOC) {
                return null;
            }

            List<List<DQLPlanNode>> childNodes = new ArrayList<>(statement.nestedStatements().size());
            SymbolBasedUpsertByIdNode upsertByIdNode = null;
            for (UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis : statement.nestedStatements()) {
                WhereClause whereClause = nestedAnalysis.whereClause();
                if (whereClause.noMatch()){
                    continue;
                }
                if (whereClause.docKeys().isPresent()) {
                    if (upsertByIdNode == null) {
                        Tuple<String[], Symbol[]> assignments = convertAssignments(nestedAnalysis.assignments());
                        upsertByIdNode = new SymbolBasedUpsertByIdNode(false, statement.nestedStatements().size() > 1, assignments.v1(), null);
                        childNodes.add(ImmutableList.<DQLPlanNode>of(upsertByIdNode));
                    }
                    upsertById(nestedAnalysis, tableInfo, whereClause, upsertByIdNode);
                } else {
                    childNodes.add(upsertByQuery(nestedAnalysis, context, tableInfo, whereClause));
                }
            }
            if (childNodes.size()>0){
                return new Upsert(childNodes);
            } else {
                return new NoopPlannedAnalyzedRelation(statement);
            }
        }

        private List<DQLPlanNode> upsertByQuery(UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis,
                                                ConsumerContext consumerContext,
                                                TableInfo tableInfo,
                                                WhereClause whereClause) {

            Symbol versionSymbol = null;
            if(whereClause.hasVersions()){
                versionSymbol = VersionRewriter.get(whereClause.query());
                whereClause = new WhereClause(whereClause.query(), whereClause.docKeys().orNull(), whereClause.partitions());
            }


            if (!whereClause.noMatch() || !(tableInfo.isPartitioned() && whereClause.partitions().isEmpty())) {
                // for updates, we always need to collect the `_uid`
                Reference uidReference = new Reference(
                        new ReferenceInfo(
                                new ReferenceIdent(tableInfo.ident(), "_uid"),
                                RowGranularity.DOC, DataTypes.STRING));

                Tuple<String[], Symbol[]> assignments = convertAssignments(nestedAnalysis.assignments());

                Long version = null;
                if (versionSymbol != null){
                    version = ValueSymbolVisitor.LONG.process(versionSymbol);
                }

                UpdateProjection updateProjection = new UpdateProjection(
                        new InputColumn(0, DataTypes.STRING),
                        assignments.v1(),
                        assignments.v2(),
                        version);

                CollectNode collectNode = PlanNodeBuilder.collect(
                        tableInfo,
                        consumerContext.plannerContext(),
                        whereClause,
                        ImmutableList.<Symbol>of(uidReference),
                        ImmutableList.<Projection>of(updateProjection),
                        null,
                        Preference.PRIMARY.type()
                );
                MergeNode mergeNode = PlanNodeBuilder.localMerge(
                        ImmutableList.<Projection>of(localMergeProjection), collectNode,
                        consumerContext.plannerContext());
                return ImmutableList.<DQLPlanNode>of(collectNode, mergeNode);
            } else {
                return ImmutableList.of();
            }
        }

        private void upsertById(UpdateAnalyzedStatement.NestedAnalyzedStatement nestedAnalysis,
                                             TableInfo tableInfo,
                                             WhereClause whereClause,
                                             SymbolBasedUpsertByIdNode upsertByIdNode) {
            String[] indices = Planner.indices(tableInfo, whereClause);
            assert indices.length == 1;

            Tuple<String[], Symbol[]> assignments = convertAssignments(nestedAnalysis.assignments());

            for (DocKeys.DocKey key : whereClause.docKeys().get()) {
                upsertByIdNode.add(
                        indices[0],
                        key.id(),
                        key.routing(),
                        assignments.v2(),
                        key.version().orNull());
            }
        }

        private Tuple<String[], Symbol[]> convertAssignments(Map<Reference, Symbol> assignments) {
            String[] assignmentColumns = new String[assignments.size()];
            Symbol[] assignmentSymbols = new Symbol[assignments.size()];
            Iterator<Reference> it = assignments.keySet().iterator();
            int i = 0;
            while(it.hasNext()) {
                Reference key = it.next();
                assignmentColumns[i] = key.ident().columnIdent().fqn();
                assignmentSymbols[i] = assignments.get(key);
                i++;
            }
            return new Tuple<>(assignmentColumns, assignmentSymbols);
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }
    }
}
