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
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.DocReferences;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.projection.ColumnIndexWriterProjection;
import io.crate.planner.projection.MergeCountProjection;
import org.elasticsearch.common.settings.Settings;

import java.util.List;


public final class InsertFromSubQueryPlanner {

    private static final ToSourceLookupConverter SOURCE_LOOKUP_CONVERTER = new ToSourceLookupConverter();

    private InsertFromSubQueryPlanner() {
    }

    public static Plan plan(InsertFromSubQueryAnalyzedStatement statement, ConsumerContext context) {

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
        QuerySpec subQuerySpec = subRelation.querySpec();

        // We'd have to enable paging & add a mergePhase to the sub-plan which applies the ordering/limit before
        // the indexWriterProjection can be added
        if (subQuerySpec.limit() != null || subQuerySpec.offset() != null || subQuerySpec.orderBy() != null) {
            throw new UnsupportedFeatureException("Using limit, offset or order by is not " +
                                                  "supported on insert using a sub-query");
        }
        SOURCE_LOOKUP_CONVERTER.process(subRelation, null);
        context.setFetchMode(FetchMode.NEVER);
        Planner.Context plannerContext = context.plannerContext();
        Plan plannedSubQuery = plannerContext.planSubRelation(subRelation, context);
        if (plannedSubQuery == null) {
            return null;
        }
        plannedSubQuery.addProjection(indexWriterProjection);
        Plan plan = Merge.ensureOnHandler(plannedSubQuery, plannerContext);
        if (plan == plannedSubQuery) {
            return plan;
        }
        plan.addProjection(MergeCountProjection.INSTANCE);
        return plan;
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
