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


import com.google.common.collect.Lists;
import io.crate.analyze.InsertFromSubQueryAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.ProjectedNode;
import io.crate.planner.node.dml.InsertNode;
import io.crate.planner.node.dml.QueryAndFetchNode;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.Reference;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;


public class InsertFromSubQueryConsumerV2 implements Consumer {

    private final static RelationVisitor RELATION_VISITOR = new RelationVisitor();
    private final static PlanVisitor PLAN_VISITOR = new PlanVisitor();

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        AnalyzedRelation relation = RELATION_VISITOR.process(context.rootRelation(), context);
        if (relation == null) {
            return false;
        }
        context.rootRelation(relation);
        return true;
    }

    private static class RelationVisitor extends io.crate.analyze.relations.RelationVisitor<ConsumerContext, AnalyzedRelation> {

        @Override
        public AnalyzedRelation visitInsertFromQuery(InsertFromSubQueryAnalyzedStatement insertFromSubQueryAnalyzedStatement, ConsumerContext context) {
            if (!(insertFromSubQueryAnalyzedStatement.subQueryRelation() instanceof PlanNode)) {
                return insertFromSubQueryAnalyzedStatement;
            }
            PlanNode subQueryRelation = (PlanNode)insertFromSubQueryAnalyzedStatement.subQueryRelation();

            List<ColumnIdent> columns = Lists.transform(insertFromSubQueryAnalyzedStatement.columns(), new com.google.common.base.Function<Reference, ColumnIdent>() {
                @Nullable
                @Override
                public ColumnIdent apply(@Nullable Reference input) {
                    if (input == null) {
                        return null;
                    }
                    return input.info().ident().columnIdent();
                }
            });
            ColumnIndexWriterProjection indexWriterProjection = new ColumnIndexWriterProjection(
                    insertFromSubQueryAnalyzedStatement.tableInfo().ident().name(),
                    insertFromSubQueryAnalyzedStatement.tableInfo().primaryKey(),
                    columns,
                    insertFromSubQueryAnalyzedStatement.primaryKeyColumnIndices(),
                    insertFromSubQueryAnalyzedStatement.partitionedByIndices(),
                    insertFromSubQueryAnalyzedStatement.routingColumn(),
                    insertFromSubQueryAnalyzedStatement.routingColumnIndex(),
                    ImmutableSettings.EMPTY,
                    insertFromSubQueryAnalyzedStatement.tableInfo().isPartitioned()
            );

            return PLAN_VISITOR.process(subQueryRelation, indexWriterProjection);
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }

    }

    static class PlanVisitor extends PlanNodeVisitor<ColumnIndexWriterProjection, PlannedAnalyzedRelation> {

        @Override
        public PlannedAnalyzedRelation visitQueryAndFetchNode(QueryAndFetchNode node, ColumnIndexWriterProjection projection) {
            assert node.localMergeNode() == null : "QueryAndFetchNode must not hold a local merge node for a insert-by-query plan";
            ProjectedNode projectedNode = new ProjectedNode(node, projection);
            return new InsertNode(Arrays.asList(node, projectedNode));
        }

        @Override
        protected PlannedAnalyzedRelation visitPlanNode(PlanNode node, ColumnIndexWriterProjection projection) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "PlanNode %s not supported", node.toString()));
        }
    }

}
