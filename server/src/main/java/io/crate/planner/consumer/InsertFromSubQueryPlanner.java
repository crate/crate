/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.consumer;


import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.AnalyzedInsertStatement;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.operators.Insert;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.types.DataTypes;


public final class InsertFromSubQueryPlanner {

    public static final String RETURNING_VERSION_ERROR_MSG =
        "Returning clause for Insert is only supported when all nodes in the cluster running at least version 4.2.0";

    private InsertFromSubQueryPlanner() {
    }

    public static LogicalPlan plan(AnalyzedInsertStatement statement,
                                   PlannerContext plannerContext,
                                   LogicalPlanner logicalPlanner,
                                   SubqueryPlanner subqueryPlanner) {

        if (statement.outputs() != null &&
            !plannerContext.clusterState().nodes().getMinNodeVersion().onOrAfter(Version.V_4_2_0)) {
            throw new UnsupportedFeatureException(RETURNING_VERSION_ERROR_MSG);
        }



        // if fields are null default to number of rows imported
        var outputs = statement.outputs() == null ? List.of(new InputColumn(0, DataTypes.LONG)) : statement.outputs();

        ColumnIndexWriterProjection indexWriterProjection = new ColumnIndexWriterProjection(
            statement.tableInfo().ident(),
            null,
            statement.tableInfo().primaryKey(),
            statement.columns(),
            statement.isIgnoreDuplicateKeys(),
            statement.onDuplicateKeyAssignments(),
            statement.primaryKeySymbols(),
            statement.partitionedBySymbols(),
            statement.tableInfo().clusteredBy(),
            statement.clusteredBySymbol(),
            Settings.EMPTY,
            statement.tableInfo().isPartitioned(),
            outputs,
            statement.outputs() == null ? List.of() : statement.outputs()
        );
        LogicalPlan plannedSubQuery = logicalPlanner.plan(
            statement.subQueryRelation(),
            plannerContext,
            subqueryPlanner,
            true
        );
        EvalProjection castOutputs = EvalProjection.castValues(
            Symbols.typeView(statement.columns()), plannedSubQuery.outputs());
        return new Insert(plannedSubQuery, indexWriterProjection, castOutputs);
    }
}
