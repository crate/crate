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
import io.crate.analyze.relations.RelationNormalizer;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.operators.Insert;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.PlanHint;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;


public final class InsertFromSubQueryPlanner {

    private InsertFromSubQueryPlanner() {
    }

    public static LogicalPlan plan(RelationNormalizer relationNormalizer,
                                   InsertFromSubQueryAnalyzedStatement statement,
                                   PlannerContext plannerContext,
                                   LogicalPlanner logicalPlanner,
                                   SubqueryPlanner subqueryPlanner) {
        final ColumnIndexWriterProjection indexWriterProjection = new ColumnIndexWriterProjection(
            statement.tableInfo().ident(),
            null,
            statement.tableInfo().primaryKey(),
            statement.columns(),
            statement.isIgnoreDuplicateKeys(),
            statement.onDuplicateKeyAssignments(),
            statement.primaryKeySymbols(),
            statement.tableInfo().partitionedBy(),
            statement.partitionedBySymbols(),
            statement.tableInfo().clusteredBy(),
            statement.clusteredBySymbol(),
            Settings.EMPTY,
            statement.tableInfo().isPartitioned()
        );

        AnalyzedRelation subRelation = relationNormalizer.normalize(
            statement.subQueryRelation(), plannerContext.transactionContext());

        LogicalPlan plannedSubQuery = logicalPlanner.normalizeAndPlan(
            subRelation, plannerContext, subqueryPlanner, FetchMode.NEVER_CLEAR, EnumSet.of(PlanHint.PREFER_SOURCE_LOOKUP));
        EvalProjection castOutputs = createCastProjection(statement.columns(), plannedSubQuery.outputs());
        return new Insert(plannedSubQuery, indexWriterProjection, castOutputs);
    }

    @Nullable
    private static EvalProjection createCastProjection(List<Reference> targetCols, List<Symbol> sourceCols) {
        ArrayList<Symbol> casts = new ArrayList<>(targetCols.size());
        boolean requiresCasts = false;
        for (int i = 0; i < sourceCols.size(); i++) {
            Symbol output = sourceCols.get(i);
            Reference targetCol = targetCols.get(i);
            InputColumn inputColumn = new InputColumn(i, output.valueType());
            DataType targetType = targetCol.valueType();
            if (targetType.id() == DataTypes.UNDEFINED.id() || targetType.equals(output.valueType())) {
                casts.add(inputColumn);
            } else {
                requiresCasts = true;
                casts.add(inputColumn.cast(targetType, false));
            }
        }
        return requiresCasts ? new EvalProjection(casts) : null;
    }
}
