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

package io.crate.planner.v2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.UpdateAnalyzedStatement;
import io.crate.analyze.VersionRewriter;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlannerContextBuilder;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dml.UpdateNode;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.crate.planner.symbol.Field.unwrap;

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

    class Visitor extends RelationVisitor<Void, PlannedAnalyzedRelation> {

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

                    PlannerContextBuilder contextBuilder = new PlannerContextBuilder()
                            .output(Lists.newArrayList((Symbol)uidReference))
                             // TODO: remove after expression evaluation moved to our new UpdateAction
                            .output(unwrap(nestedAnalysis.assignments().values()));

                    UpdateProjection updateProjection = new UpdateProjection(
                            nestedAnalysis.assignments(),
                            contextBuilder.outputs(),
                            whereClauseContext.whereClause().version().orNull());

                    CollectNode collectNode = PlanNodeBuilder.collect(
                            tableInfo,
                            whereClause,
                            contextBuilder.toCollect(),
                            ImmutableList.<Projection>of(updateProjection)
                    );
                    MergeNode mergeNode = PlanNodeBuilder.localMerge(
                            ImmutableList.<Projection>of(localMergeProjection), collectNode);
                    childNodes.add(ImmutableList.<DQLPlanNode>of(collectNode, mergeNode));
                }
            }

            return new UpdateNode(childNodes);
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            return null;
        }
    }
}
