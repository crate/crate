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


import io.crate.analyze.InsertFromSubQueryAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.DocReferences;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.operators.Insert;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import org.elasticsearch.common.settings.Settings;

import java.util.List;


public final class InsertFromSubQueryPlanner {

    private static final ToSourceLookupConverter SOURCE_LOOKUP_CONVERTER = new ToSourceLookupConverter();

    private InsertFromSubQueryPlanner() {
    }

    public static LogicalPlan plan(InsertFromSubQueryAnalyzedStatement statement,
                                   PlannerContext plannerContext,
                                   LogicalPlanner logicalPlanner,
                                   SubqueryPlanner subqueryPlanner) {
        final ColumnIndexWriterProjection indexWriterProjection = new ColumnIndexWriterProjection(
            statement.tableInfo().ident(),
            null,
            statement.tableInfo().primaryKey(),
            statement.columns(),
            statement.onDuplicateKeyAssignments(),
            statement.primaryKeySymbols(),
            statement.tableInfo().partitionedBy(),
            statement.partitionedBySymbols(),
            statement.tableInfo().clusteredBy(),
            statement.clusteredBySymbol(),
            Settings.EMPTY,
            statement.tableInfo().isPartitioned()
        );

        QueriedRelation subRelation = statement.subQueryRelation();

        // We'd have to enable paging & add a mergePhase to the sub-plan which applies the ordering/limit before
        // the indexWriterProjection can be added
        if (subRelation.limit() != null || subRelation.offset() != null || subRelation.orderBy() != null) {
            throw new UnsupportedFeatureException("Using limit, offset or order by is not " +
                                                  "supported on insert using a sub-query");
        }
        SOURCE_LOOKUP_CONVERTER.process(subRelation, null);
        LogicalPlan plannedSubQuery = logicalPlanner.plan(subRelation, plannerContext, subqueryPlanner, FetchMode.NEVER_CLEAR);
        return new Insert(plannedSubQuery, indexWriterProjection);
    }

    private static class ToSourceLookupConverter extends AnalyzedRelationVisitor<Void, Void> {

        @Override
        public Void visitQueriedDocTable(QueriedDocTable table, Void context) {
            if (table.querySpec().hasAggregates() || !table.querySpec().groupBy().isEmpty()) {
                return null;
            }

            List<Symbol> outputs = table.querySpec().outputs();
            assert table.querySpec().orderBy() == null : "insert from subquery with order by is not supported";
            for (int i = 0; i < outputs.size(); i++) {
                outputs.set(i, DocReferences.toSourceLookup(outputs.get(i)));
            }
            return null;
        }

        @Override
        protected Void visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            return null;
        }
    }
}
