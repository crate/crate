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
import com.google.common.collect.Sets;
import io.crate.analyze.InsertFromSubQueryAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.GeneratedReferenceInfo;
import io.crate.metadata.ReferenceInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.Planner;
import io.crate.planner.node.dml.InsertFromSubQuery;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.ColumnIndexWriterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.InputCreatingVisitor;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.List;


public class InsertFromSubQueryConsumer implements Consumer {

    private static final InputCreatingVisitor INPUT_CREATING_VISITOR = InputCreatingVisitor.INSTANCE;
    private static final Visitor VISITOR = new Visitor();

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation relation, ConsumerContext context) {
        return VISITOR.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private static final ToSourceLookupConverter SOURCE_LOOKUP_CONVERTER = new ToSourceLookupConverter();

        @Override
        public PlannedAnalyzedRelation visitInsertFromQuery(InsertFromSubQueryAnalyzedStatement statement,
                                                            ConsumerContext context) {

            ColumnIndexWriterProjection indexWriterProjection = new ColumnIndexWriterProjection(
                    statement.tableInfo().ident(),
                    null,
                    statement.tableInfo().primaryKey(),
                    statement.columns(),
                    statement.onDuplicateKeyAssignments(),
                    statement.primaryKeyColumnIndices(),
                    statement.partitionedByIndices(),
                    statement.routingColumn(),
                    statement.routingColumnIndex(),
                    ImmutableSettings.EMPTY,
                    statement.tableInfo().isPartitioned()
            );

            InputCreatingVisitor.Context inputContext = new InputCreatingVisitor.Context(statement.columns());
            for (ReferenceInfo referenceInfo : statement.tableInfo().partitionedByColumns()) {
                if (referenceInfo instanceof GeneratedReferenceInfo && !statement.containsReferenceInfo(referenceInfo)) {
                    GeneratedReferenceInfo generatedReferenceInfo = (GeneratedReferenceInfo) referenceInfo;
                    Symbol expression = INPUT_CREATING_VISITOR.process(generatedReferenceInfo.generatedExpression(), inputContext);
                    indexWriterProjection.partitionedBySymbols().add(expression);
                }
            }

            Planner.Context plannerContext = context.plannerContext();
            SOURCE_LOOKUP_CONVERTER.process(statement.subQueryRelation(), null);
            PlannedAnalyzedRelation plannedSubQuery = plannerContext.planSubRelation(statement.subQueryRelation(), context);
            if (plannedSubQuery == null) {
                return null;
            }

            plannedSubQuery.addProjection(indexWriterProjection);

            MergePhase mergeNode = null;
            if (plannedSubQuery.resultIsDistributed()) {
                // add local merge Node which aggregates the distributed results
                AggregationProjection aggregationProjection = CountAggregation.PARTIAL_COUNT_AGGREGATION_PROJECTION;
                mergeNode = MergePhase.localMerge(
                        plannerContext.jobId(),
                        plannerContext.nextExecutionPhaseId(),
                        ImmutableList.<Projection>of(aggregationProjection),
                        plannedSubQuery.resultPhase().executionNodes().size(),
                        Symbols.extractTypes(indexWriterProjection.outputs()));
                mergeNode.executionNodes(Sets.newHashSet(plannerContext.clusterService().localNode().id()));
            }
            return new InsertFromSubQuery(plannedSubQuery.plan(), mergeNode, plannerContext.jobId());
        }
    }

    private static class ToSourceLookupConverter extends AnalyzedRelationVisitor<Void, Void> {

        @Override
        public Void visitQueriedDocTable(QueriedDocTable table, Void context) {
            if (table.querySpec().hasAggregates() || table.querySpec().groupBy().isPresent()) {
                return null;
            }

            List<Symbol> outputs = table.querySpec().outputs();
            assert !table.querySpec().orderBy().isPresent() : "insert from subquery with order by is not supported";
            for (int i = 0; i < outputs.size(); i++) {
                outputs.set(i, DocReferenceConverter.convertIf(outputs.get(i)));
            }
            return null;
        }

        @Override
        protected Void visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            return null;
        }
    }
}
